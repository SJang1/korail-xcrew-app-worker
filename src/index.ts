import { KorailClient, TrainClient } from './korail';
import { createSession, destroySession, verifySession, deleteUserAccount } from './auth';
import { mapConcurrent, generateColor } from './utils';
import { FCM, FcmOptions, EnhancedFcmMessage } from 'fcm-cloudflare-workers';


interface PushNotification {
    id: string;
    username: string;
    fcmToken: string;
    trainNo: string;
    driveDate: string;
    stationName: string;
    stationIndex: number;
}

interface WorkPsttQueueMessage {
    tag: 'work-pstt-fetch';
    username: string;
    xcrewPw: string;
    empName: string;
    date: string;
}


export default {

    async queue(batch: MessageBatch<WorkPsttQueueMessage>, env: Env, ctx: ExecutionContext): Promise<void> {
        console.log(`Processing ${batch.messages.length} work pstt fetch requests`);
        
        // Group messages by credentials to avoid multiple authentication attempts
        const messagesByCredentials = new Map<string, Message<WorkPsttQueueMessage>[]>();
        
        for (const message of batch.messages) {
            const { tag, username, xcrewPw } = message.body;
            
            // Validate message tag
            if (tag !== 'work-pstt-fetch') {
                console.error(`Invalid message tag: ${tag}. Expected 'work-pstt-fetch'. Skipping message.`);
                message.ack();
                continue;
            }
            
            const credKey = `${username}:${xcrewPw}`;
            const group = messagesByCredentials.get(credKey) ?? [];
            group.push(message);
            messagesByCredentials.set(credKey, group);
        }
        
        // Process each credential group with a single authenticated session
        for (const [credKey, messages] of messagesByCredentials) {
            const { username, xcrewPw, empName } = messages[0].body;
            
            try {
                // Create one client instance for all messages with same credentials
                const client = new KorailClient(username, xcrewPw);
                
                // Process all dates for this user sequentially to avoid session conflicts
                for (const message of messages) {
                    try {
                        const { date } = message.body;
                        const workPsttData = await client.getSearchExtrCrewWrkPstt(date, empName);
                        
                        if (workPsttData && workPsttData.data && Array.isArray(workPsttData.data)) {
                            for (const item of workPsttData.data) {
                                await env.DB.prepare(
                                    "INSERT OR REPLACE INTO searchExtrCrewWrkPstt (username, pjtDt, pdiaNo, repTrn1No, gwkTm, repTrn2No, loiwTm, empNm1, empNm2, empNm3, empNm4) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                                )
                                    .bind(username, date, item.pdiaNo, item.repTrn1No.trim(), item.gwkTm, item.repTrn2No.trim(), item.loiwTm, item.empNm1.trim(), item.empNm2.trim(), item.empNm3.trim(), item.empNm4.trim())
                                    .run();
                            }
                            console.log(`Saved ${workPsttData.data.length} workPstt records for ${username} on ${date}`);
                        }
                        
                        message.ack();
                    } catch (e: any) {
                        console.error(`Failed to fetch work pstt for ${message.body.date}:`, e.message);
                        // Let the message retry with default retry policy
                    }
                }
            } catch (e: any) {
                console.error(`Failed to create client for ${username}:`, e.message);
                // If client creation fails, all messages in this group will retry
            }
        }
    },

    async scheduled(controller: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {

        // init FCM with access token caching using KV (optional but recommended for performance)
        const fcmOptions = new FcmOptions({
        serviceAccount: JSON.parse(env.FIREBASE_SERVICE_ACCOUNT_JSON),
        // Specify a KV namespace
        kvStore: env.PUSH_KV,
        // Specify a key to use for caching the access token
        kvCacheKey: 'fcm_access_token',
    });

const fcm = new FCM(fcmOptions);
        console.log("Cron processed: checking push notifications");
        const { results } = await env.DB.prepare("SELECT * FROM push_notifications").all<PushNotification>();
        if (!results || results.length === 0) {
            console.log("No push notification requests to process.");
            return;
        }



        // Group alert requests by unique train (trainNo + driveDate) to avoid duplicate API calls.
        const requestsByTrain = results.reduce((acc, record) => {
            const key = `${record.trainNo}|${record.driveDate}`;
            const group = acc.get(key) ?? [];
            group.push(record);
            acc.set(key, group);
            return acc;
        }, new Map<string, PushNotification[]>());

        const client = new TrainClient();
        const apiToken = await env.NXLOGIS_SECRET.get();
        const recordsToDelete: string[] = [];
        const notificationPromises: Promise<any>[] = [];

        // Fetch train data concurrently
        const trainDataPromises = Array.from(requestsByTrain.keys()).map(async (key) => {
            const [trainNo, driveDate] = key.split('|');
            try {
                const trainData = await client.getTrainData(trainNo, driveDate, apiToken);
                return { key, trainData };
            } catch (e: any) {
                console.error(`Failed to fetch train data for ${key}:`, e.message);
                return { key, trainData: null };
            }
        });

        const trainDataResults = await Promise.all(trainDataPromises);

        // Process notifications based on fetched data
        for (const { key, trainData } of trainDataResults) {
            if (!trainData || !trainData.found || !trainData.schedule || trainData.schedule.length === 0) continue;

            const records = requestsByTrain.get(key)!;
            for (const record of records) {
                let stationIndex = record.stationIndex;
                let messageBody = "";
                let shouldDelete = false;

                // Validate stationIndex and stationName
                if (stationIndex === undefined ||
                    !trainData.schedule[stationIndex] ||
                    trainData.schedule[stationIndex].stationName !== record.stationName) {
                    
                    console.warn(`Station index/name mismatch for train ${record.trainNo}. DB says index ${record.stationIndex} is ${record.stationName}, but API has ${trainData.schedule[stationIndex]?.stationName}. Searching for a better match.`);
                    
                    // Fallback search: find the first match at or after the stored index
                    const newIndex = trainData.schedule.findIndex((info: any, idx: number) =>
                        idx >= (record.stationIndex || 0) && info.stationName === record.stationName
                    );

                    if (newIndex !== -1) {
                        stationIndex = newIndex;
                    } else {
                        // Could not definitively find station
                        console.error(`Could not definitively find station ${record.stationName} for train ${record.trainNo}. Sending alert.`);
                        messageBody = `${record.trainNo} 열차의 ${record.stationName}역 정보가 현재 조회되지 않습니다. 잠시 후 다시 시도해주세요.`;
                        shouldDelete = true; // Still delete the record as it's an unresolvable request for now.
                    }
                }

                // If stationIndex is still -1 after fallback or if messageBody was set due to "not found"
                if (stationIndex === -1 && !messageBody) {
                    continue; // Skip if no valid station found and no error message was generated.
                }

                if (!messageBody) { // Only proceed to normal logic if no "not found" message is pending
                    const isLastStation = stationIndex === trainData.schedule.length - 1;
                    const currentStationInfo = trainData.schedule[stationIndex];
                    const departureTime = currentStationInfo.actualDepartureTime || currentStationInfo.scheduledDepartureTime;
                    const arrivalTime = currentStationInfo.actualArrivalTime || currentStationInfo.scheduledArrivalTime;
    
                    if (isLastStation) {
                        if (arrivalTime) {
                            messageBody = `${record.trainNo} 열차가 종착역인 ${record.stationName}역에 도착했습니다.\n도착시간: ${arrivalTime}\n운행일자: ${trainData.info?.driveDate}`;
                            shouldDelete = true;
                        }
                    } else {
                        const hasDeparted = !!departureTime && departureTime !== "00:00:00";
    
                        if (hasDeparted) {
                            messageBody = `${record.trainNo} 열차가 ${record.stationName}역에서 출발했습니다.\n출발시간: ${departureTime}\n운행일자: ${trainData.info?.driveDate}`;
                            shouldDelete = true;
                        } else {
                            // Check if it has passed the station without a record
                            const subsequentStations = trainData.schedule.slice(stationIndex + 1);
                            const nextDepartedStation = subsequentStations.find((info: any) => {
                                const depTime = info.actualDepartureTime || info.scheduledDepartureTime;
                                return depTime && depTime !== "00:00:00";
                            });
    
                            if (nextDepartedStation) {
                                const nextStationName = nextDepartedStation.stationName;
                                const nextDepartureTime = nextDepartedStation.actualDepartureTime || nextDepartedStation.scheduledDepartureTime;
                                messageBody = `${record.trainNo} 열차는 ${record.stationName}역을 통과한 것으로 보입니다 (시간 기록 없음).\n${nextStationName}역에서 ${nextDepartureTime}에 출발했습니다.\n운행일자: ${trainData.info?.driveDate}`;
                                shouldDelete = true;
                            }
                        }
                    }
                }
                
                if (messageBody) {
                    if (shouldDelete) {
                        recordsToDelete.push(record.id);
                    }
                    const message: EnhancedFcmMessage = {
                        notification: {
                            title: "열차 상태 알림",
                            body: messageBody,
                        },
                        data: {
                            trainNo: record.trainNo,
                            driveDate: record.driveDate,
                            stationName: record.stationName,
                        }
                    };
                    notificationPromises.push(fcm.sendToToken(message, record.fcmToken));
                    console.log(`Queued notification for train ${record.trainNo} at ${record.stationName} for user ${record.username}`);
                }
            }
        }

        // Send all notifications and delete records
        if (notificationPromises.length > 0) {
            try {
                await Promise.all(notificationPromises);
                console.log(`Successfully sent ${notificationPromises.length} notifications.`);

                const placeholders = recordsToDelete.map(() => '?').join(',');
                const stmt = `DELETE FROM push_notifications WHERE id IN (${placeholders})`;
                await env.DB.prepare(stmt).bind(...recordsToDelete).run();
                console.log(`Deleted ${recordsToDelete.length} processed records.`);
            } catch (e: any) {
                console.error("Error sending notifications or deleting records:", e.message);
            }
        } else {
            console.log("No trains met the departure criteria in this run.");
        }
    },

	async fetch(request: Request, env: Env): Promise<Response> {
		const url = new URL(request.url);
        const path = url.pathname;
        const method = request.method;

        let session: { username: string, isAdmin: boolean } | null = null;

		if (path.startsWith("/api/")) {
            // CORS headers
            const corsHeaders = {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, HEAD, POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type, Authorization",
            };

            if (method === "OPTIONS") {
                return new Response(null, { headers: corsHeaders });
            }

            const secret = await env.JWT_SECRET.get() || "default-dev-secret-change-me";
            
            // --- Unified Authentication Middleware ---
            // Skip auth for login/register routes
            if (path !== "/api/admin/login" && path !== "/api/auth/login" && path !== "/api/auth/register") {
                session = await verifySession(env.KORAIL_XCREW_SESSION_KV, request, secret);

                // Admin route protection
                if (path.startsWith("/api/admin/") && !session?.isAdmin) {
                    return new Response("Unauthorized: Admin access required", { status: 401, headers: corsHeaders });
                }

                // General protected route protection
                if ((path.startsWith("/api/xcrew/") || path.startsWith("/api/train") || path.startsWith("/api/user") || path === "/api/auth/logout") && !session) {
                    return new Response("Unauthorized: Invalid or expired session", { status: 401, headers: corsHeaders });
                }
            }

            try {
                // --- Admin Auth Endpoints ---
                if (path === "/api/admin/login" && method === "POST") {
                     const { username, password } = await request.json() as any;
                     if (!username || !password) return new Response("Missing fields", { status: 400, headers: corsHeaders });

                     const msgBuffer = new TextEncoder().encode(password);
                     const hashBuffer = await crypto.subtle.digest('SHA-256', msgBuffer);
                     const hashHex = Array.from(new Uint8Array(hashBuffer)).map(b => b.toString(16).padStart(2, '0')).join('');

                     const admin = await env.DB.prepare("SELECT * FROM admins WHERE username = ?")
                        .bind(username)
                        .first();

                     if (!admin || admin.password_hash !== hashHex) {
                         return new Response("Invalid admin credentials", { status: 401, headers: corsHeaders });
                     }
                     
                     // Create Admin Session
                     const token = await createSession(env.KORAIL_XCREW_SESSION_KV, admin.username as string, secret, "admin:");
 
                     const cookie = `admin_token=${token}; Path=/; Expires=${new Date(Date.now() + 1000 * 60 * 60 * 24 * 365).toUTCString()}; HttpOnly; Secure; SameSite=Strict`;
 
                     const headers = { ...corsHeaders, "Set-Cookie": cookie };
 
                     return Response.json({ 
                         success: true, 
                         admin: { username: admin.username }
                     }, { headers });
                }

                if (path === "/api/admin/logout" && method === "POST") {
                    if (session?.isAdmin) {
                        await destroySession(env.KORAIL_XCREW_SESSION_KV, session.username, "admin:");
                    }
                    const cookie = `admin_token=; Path=/; Expires=Thu, 01 Jan 1970 00:00:00 GMT; HttpOnly; Secure; SameSite=Strict`;
                    const headers = { ...corsHeaders, "Set-Cookie": cookie };
                    return Response.json({ success: true }, { headers });
                }

                if (path === "/api/admin/password" && method === "POST") {
                    if (!session) return new Response("Unauthorized", { status: 401, headers: corsHeaders });
                    const { currentPassword, newPassword } = await request.json() as any;
                    if (!currentPassword || !newPassword) return new Response("Missing fields", { status: 400, headers: corsHeaders });

                    const admin = await env.DB.prepare("SELECT password_hash FROM admins WHERE username = ?").bind(session.username).first();
                    if (!admin) return new Response("Admin not found", { status: 404, headers: corsHeaders });

                    // Verify current
                    const currentHashBuffer = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(currentPassword));
                    const currentHashHex = Array.from(new Uint8Array(currentHashBuffer)).map(b => b.toString(16).padStart(2, '0')).join('');

                    if (admin.password_hash !== currentHashHex) {
                        return new Response("Invalid current password", { status: 403, headers: corsHeaders });
                    }

                    // Update
                    const newHashBuffer = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(newPassword));
                    const newHashHex = Array.from(new Uint8Array(newHashBuffer)).map(b => b.toString(16).padStart(2, '0')).join('');

                    await env.DB.prepare("UPDATE admins SET password_hash = ? WHERE username = ?")
                        .bind(newHashHex, session.username)
                        .run();
                    
                    return Response.json({ success: true, message: "Password updated" }, { headers: corsHeaders });
                }

                if (path === "/api/admin/admins" && method === "GET") {
                    const { results } = await env.DB.prepare("SELECT id, username, created_at FROM admins ORDER BY created_at DESC").all();
                    return Response.json({ success: true, data: results }, { headers: corsHeaders });
                }

                if (path === "/api/admin/create" && method === "POST") {
                    const { username, password } = await request.json() as any;
                    if (!username || !password) return new Response("Missing fields", { status: 400, headers: corsHeaders });

                    const hashBuffer = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(password));
                    const hashHex = Array.from(new Uint8Array(hashBuffer)).map(b => b.toString(16).padStart(2, '0')).join('');

                    try {
                        await env.DB.prepare("INSERT INTO admins (username, password_hash) VALUES (?, ?)")
                            .bind(username, hashHex)
                            .run();
                        return Response.json({ success: true }, { headers: corsHeaders });
                    } catch (e: any) {
                        if (e.message.includes("UNIQUE")) {
                            return new Response("Username already exists", { status: 409, headers: corsHeaders });
                        }
                        throw e;
                    }
                }

                if (path.startsWith("/api/admin/admins/") && method === "DELETE") {
                    if (!session) return new Response("Unauthorized", { status: 401, headers: corsHeaders });
                    const targetAdmin = path.split("/").pop(); // /api/admin/admins/:username
                    if (!targetAdmin) return new Response("Missing username", { status: 400, headers: corsHeaders });

                    if (targetAdmin === session.username) {
                        return new Response("Cannot delete yourself", { status: 400, headers: corsHeaders });
                    }

                    await env.DB.prepare("DELETE FROM admins WHERE username = ?").bind(targetAdmin).run();
                    return Response.json({ success: true }, { headers: corsHeaders });
                }


                // --- User Management Endpoints ---
                if (path === "/api/user/profile" && (method === "GET" || method === "POST")) {
                    if (!session) return new Response("Unauthorized", { status: 401, headers: corsHeaders });
                    if (method === "GET") {
                        let targetUsername: string | null = null;
                        const requestedUsername = url.searchParams.get("username");

                        if (session.isAdmin) {
                            targetUsername = requestedUsername || session.username; // Default to self if no user specified
                        } else {
                            targetUsername = session.username;
                             if (requestedUsername && requestedUsername !== targetUsername) {
                                return new Response("Unauthorized: You can only access your own data.", { status: 403, headers: corsHeaders });
                            }
                        }
                        
                        const user = await env.DB.prepare("SELECT username, name FROM users WHERE username = ?").bind(targetUsername).first();
                        if (!user) return new Response("User not found", { status: 404, headers: corsHeaders });
                        return Response.json({ success: true, data: { name: user.name } }, { headers: corsHeaders });
                    }

                    if (method === "POST") {
                        // POST should always be for the logged-in user themselves.
                        const { name } = await request.json() as any;
                        if (typeof name !== 'string') return new Response("Invalid name", { status: 400, headers: corsHeaders });

                        await env.DB.prepare("UPDATE users SET name = ? WHERE username = ?")
                            .bind(name, session.username)
                            .run();
                        
                        return Response.json({ success: true, message: "Profile updated" }, { headers: corsHeaders });
                    }
                }

                if (path === "/api/user/password" && method === "POST") {
                    if (!session) return new Response("Unauthorized", { status: 401, headers: corsHeaders });
                    const { currentPassword, newPassword } = await request.json() as any;
                    if (!currentPassword || !newPassword) return new Response("Missing fields", { status: 400, headers: corsHeaders });

                    // 1. Verify current password
                    const user = await env.DB.prepare("SELECT password_hash FROM users WHERE username = ?").bind(session.username).first();
                    if (!user) return new Response("User not found", { status: 404, headers: corsHeaders });

                    const currentHashBuffer = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(currentPassword));
                    const currentHashHex = Array.from(new Uint8Array(currentHashBuffer)).map(b => b.toString(16).padStart(2, '0')).join('');

                    if (user.password_hash !== currentHashHex) {
                        return new Response("Invalid current password", { status: 403, headers: corsHeaders });
                    }

                    // 2. Hash and update to new password
                    const newHashBuffer = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(newPassword));
                    const newHashHex = Array.from(new Uint8Array(newHashBuffer)).map(b => b.toString(16).padStart(2, '0')).join('');

                    await env.DB.prepare("UPDATE users SET password_hash = ? WHERE username = ?")
                        .bind(newHashHex, session.username)
                        .run();
                    
                    return Response.json({ success: true, message: "Password updated successfully" }, { headers: corsHeaders });
                }


                // --- Auth Endpoints (D1) ---
                
                if (path === "/api/auth/register" && method === "POST") {
                    const { username, password, xcrewPassword } = await request.json() as any;
                    if (!username || !password) return new Response("Missing fields", { status: 400, headers: corsHeaders });
                    
                    if (!xcrewPassword) {
                         return new Response("Xcrew password required for verification", { status: 400, headers: corsHeaders });
                    }

                    // Verify Xcrew Credentials
                    const client = new KorailClient(username, xcrewPassword);
                    try {
                        await client.authenticate();
                    } catch (e: any) {
                        return new Response(`Xcrew Verification Failed: ${e.message}`, { status: 401, headers: corsHeaders });
                    }

                    // Simple hash (In prod use proper scrypt/argon2)
                    const msgBuffer = new TextEncoder().encode(password);
                    const hashBuffer = await crypto.subtle.digest('SHA-256', msgBuffer);
                    const hashArray = Array.from(new Uint8Array(hashBuffer));
                    const hashHex = hashArray.map(b => b.toString(16).padStart(2, '0')).join('');

                    try {
                        await env.DB.prepare("INSERT INTO users (username, password_hash) VALUES (?, ?)")
                            .bind(username, hashHex)
                            .run();
                        return Response.json({ success: true }, { headers: corsHeaders });
                    } catch (e: any) {
                         if (e.message.includes("UNIQUE")) {
                             return new Response("Username already exists", { status: 409, headers: corsHeaders });
                         }
                         throw e;
                    }
                }

                if (path === "/api/auth/login" && method === "POST") {
                     const { username, password } = await request.json() as any;
                     if (!username || !password) return new Response("Missing fields", { status: 400, headers: corsHeaders });

                     const msgBuffer = new TextEncoder().encode(password);
                     const hashBuffer = await crypto.subtle.digest('SHA-256', msgBuffer);
                     const hashHex = Array.from(new Uint8Array(hashBuffer)).map(b => b.toString(16).padStart(2, '0')).join('');

                     const user = await env.DB.prepare("SELECT * FROM users WHERE username = ?")
                        .bind(username)
                        .first();

                     if (!user) {
                         console.log(`Login failed: User '${username}' not found.`);
                         return new Response("User not found (Did you register on this environment?)", { status: 401, headers: corsHeaders });
                     }
                     
                     if (user.password_hash !== hashHex) {
                         console.log(`Login failed: Password mismatch for '${username}'.`);
                         return new Response("Invalid password", { status: 401, headers: corsHeaders });
                     }
                     
                     // Create Session (KV + JWT)
                     // Use a default secret if env is missing (for dev safety, though should be set)
                     const secret = await env.JWT_SECRET.get() || "default-dev-secret-change-me";
                     const token = await createSession(env.KORAIL_XCREW_SESSION_KV, user.username as string, secret);
 
                     const cookie = `auth_token=${token}; Path=/; Expires=${new Date(Date.now() + 1000 * 60 * 60 * 24 * 365).toUTCString()}; HttpOnly; Secure; SameSite=Strict`;
 
                     const headers = { ...corsHeaders, "Set-Cookie": cookie };
 
                     return Response.json({ 
                         success: true, 
                         user: { username: user.username, name: user.name }
                     }, { headers });
                }

                if (path === "/api/auth/logout" && method === "POST") {
                    if (session && !session.isAdmin) {
                        await destroySession(env.KORAIL_XCREW_SESSION_KV, session.username);
                    }
                    // Clear the cookie by setting an expiry date in the past
                    const cookie = `auth_token=; Path=/; Expires=Thu, 01 Jan 1970 00:00:00 GMT; HttpOnly; Secure; SameSite=Strict`;
                    const headers = { ...corsHeaders, "Set-Cookie": cookie };
                    
                    // Always return success, client will clear local storage regardless
                    return Response.json({ success: true }, { headers });
                }

                if (path === "/api/auth/cancel" && method === "DELETE") {
                    if (!session) return new Response("Unauthorized", { status: 401, headers: corsHeaders });
                    await deleteUserAccount(env.DB, env.KORAIL_XCREW_SESSION_KV, session.username);

                    const cookie = `auth_token=; Path=/; Expires=Thu, 01 Jan 1970 00:00:00 GMT; HttpOnly; Secure; SameSite=Strict`;
                    return Response.json({ success: true, message: "Account deleted successfully" }, { headers: { ...corsHeaders, "Set-Cookie": cookie } });
                }

                // --- Xcrew Proxy Endpoints ---
                
                if (path === "/api/xcrew/schedule" && method === "GET") {
                    if (!session) return new Response("Unauthorized", { status: 401, headers: corsHeaders });
                    let targetUsername: string | null = null;
                    const requestedUsername = url.searchParams.get("username");
                    
                    if (session.isAdmin) {
                        targetUsername = requestedUsername;
                    } else {
                        targetUsername = session.username;
                        // Optional: if a non-admin tries to request another user's data
                        if (requestedUsername && requestedUsername !== targetUsername) {
                            return new Response("Unauthorized: You can only access your own data.", { status: 403, headers: corsHeaders });
                        }
                    }

                    const date = url.searchParams.get("date"); // YYYYMMDD
                    if (!targetUsername || !date) return new Response("Missing params", { status: 400, headers: corsHeaders });

                    const row = await env.DB.prepare("SELECT data FROM schedules WHERE username = ? AND date = ?")
                        .bind(targetUsername, date)
                        .first();
                        
                    let scheduleData = row ? JSON.parse(row.data as string) : null;

                    // Merge working locations
                    if (scheduleData && Array.isArray(scheduleData)) {
                        const monthPrefix = date.substring(0, 6); // YYYYMM
                        const { results: locs } = await env.DB.prepare("SELECT date, location FROM working_locations WHERE username = ? AND date LIKE ?")
                            .bind(targetUsername, `${monthPrefix}%`)
                            .all();
                        
                        const locMap = (locs || []).reduce((acc: any, curr: any) => {
                            acc[curr.date] = curr.location;
                            return acc;
                        }, {});

                        scheduleData = scheduleData.map((item: any) => {
                            if (locMap[item.pjtDt]) {
                                item.location = locMap[item.pjtDt];
                            }
                            return item;
                        });
                    }
                        
                    // Also fetch colors map
                    const { results: colors } = await env.DB.prepare("SELECT * FROM location_colors").all();
                    const colorMap = (colors || []).reduce((acc: any, curr: any) => {
                        acc[curr.name] = curr.color;
                        return acc;
                    }, {});
                    
                    colorMap['비상'] = 'hsl(0, 0%, 94%)';

                    return Response.json({ 
                        success: true, 
                        data: scheduleData,
                        colors: colorMap
                    }, { headers: corsHeaders });
                }

                if (path === "/api/xcrew/schedule" && method === "POST") {
                    if (!session) return new Response("Unauthorized", { status: 401, headers: corsHeaders });
                    const { xcrewId, xcrewPw, date, empName } = await request.json() as any;
                    if (!xcrewId || !xcrewPw || !date || !empName) {
                        return new Response("Missing params", { status: 400, headers: corsHeaders });
                    }

                    // For updates, the logged-in user must match the requested user, even for admins.
                    if (xcrewId !== session.username) {
                        return new Response("Unauthorized: You can only update your own schedule.", { status: 403, headers: corsHeaders });
                    }

                    const client = new KorailClient(xcrewId, xcrewPw);
                    try {
                        // Helper function to generate all dates in the month
                        const getDateRangeInMonth = (dateStr: string): string[] => {
                            const year = parseInt(dateStr.substring(0, 4));
                            const month = parseInt(dateStr.substring(4, 6));
                            const firstDay = new Date(year, month - 1, 1);
                            const lastDay = new Date(year, month, 0);
                            const dates: string[] = [];
                            
                            for (let d = new Date(firstDay); d <= lastDay; d.setDate(d.getDate() + 1)) {
                                const yyyy = d.getFullYear().toString();
                                const mm = (d.getMonth() + 1).toString().padStart(2, '0');
                                const dd = d.getDate().toString().padStart(2, '0');
                                dates.push(`${yyyy}${mm}${dd}`);
                            }
                            return dates;
                        };

                        const dateRange = getDateRangeInMonth(date);
                        
                        // Queue workPsttData fetch requests for all dates in the month
                        const queueMessages: WorkPsttQueueMessage[] = dateRange.map(currentDate => ({
                            tag: 'work-pstt-fetch',
                            username: xcrewId,
                            xcrewPw: xcrewPw,
                            empName: empName,
                            date: currentDate
                        }));
                        
                        await env.WORK_PSTT_QUEUE.sendBatch(queueMessages.map(msg => ({ body: msg })));
                        console.log(`Queued ${queueMessages.length} work pstt fetch requests for ${xcrewId}`);

                        // 1. Fetch Schedule
                        const schedule = await client.getSchedule(date, empName);
                        
                        // 2. Identify working days to update
                        // Filter: pdiaNo exists, is not "S", and does not start with "~"
                        const workingDays = schedule.filter((item: any) => {
                            const p = item.pdiaNo;
                            return p && p !== "S" && !p.startsWith("~");
                        });

                        // 3. Concurrent Dia Fetch & Processing
                        const concurrency = 5; // Conservative limit
                        
                        // Helper to process one day
                        const processDay = async (item: any) => {
                            try {
                                // Pass known pdiaNo to skip lookup
                                const dia = await client.getDiaInfo(item.pjtDt, item.pdiaNo);
                                if (!dia) return;

                                // Save Dia to D1
                                await env.DB.prepare("INSERT OR REPLACE INTO dia_info (username, date, data) VALUES (?, ?, ?)")
                                    .bind(xcrewId, item.pjtDt, JSON.stringify(dia))
                                    .run();
                                
                                console.log(`Saved Dia for ${item.pjtDt}`);

                                // Extract Location (Logic: Start == End ? Start : Start)
                                let location = "";
                                const segments = dia.data || dia.extrCrewDiaList || [];
                                
                                if (segments.length > 0) {
                                    // Check for special '비상' case
                                    // If the first segment is '비상' and it's the only one or dominant logic
                                    if (segments[0].pjtHrDvNm === "비상") {
                                        location = "비상";
                                    } else {
                                        let startStation = "";
                                        
                                        // Find first valid start station
                                        for (const seg of segments) {
                                            const nm = seg.dptStnNm || seg.depStnNm;
                                            if (nm) {
                                                startStation = nm;
                                                break;
                                            }
                                        }
                                        
                                        if (startStation) {
                                            location = startStation;
                                        }
                                    }
                                    
                                    if (location) {
                                        // Save to working_locations
                                        await env.DB.prepare("INSERT OR REPLACE INTO working_locations (username, date, location) VALUES (?, ?, ?)")
                                            .bind(xcrewId, item.pjtDt, location)
                                            .run();
                                    }
                                    
                                    console.log(`Date: ${item.pjtDt}, Segments: ${segments.length}, Location: ${location}`);
                                } else {
                                    console.log(`Date: ${item.pjtDt}, No segments found.`);
                                }

                                if (location) {
                                    item.location = location; // Enrich schedule item

                                    // Handle Color
                                    let colorRow = await env.DB.prepare("SELECT color FROM location_colors WHERE name = ?")
                                        .bind(location)
                                        .first();
                                    
                                    if (!colorRow) {
                                        // Generate unique color
                                        let newColor = generateColor(location);
                                        let retries = 0;
                                        
                                        while (retries < 10) {
                                            const existing = await env.DB.prepare("SELECT 1 FROM location_colors WHERE color = ?")
                                                .bind(newColor)
                                                .first();
                                            if (!existing) break;
                                            
                                            // Shift hue if collision
                                            const hue = Math.floor(Math.random() * 360);
                                            newColor = `hsl(${hue}, 70%, 85%)`;
                                            retries++;
                                        }

                                        await env.DB.prepare("INSERT INTO location_colors (name, color) VALUES (?, ?)")
                                            .bind(location, newColor)
                                            .run();
                                    }
                                }
                            } catch (e) {
                                console.error(`Failed to process dia for ${item.pjtDt}`, e);
                            }
                        };

                        await mapConcurrent(workingDays, concurrency, processDay);
                        
                        // 4. Fetch all colors
                        const { results: colors } = await env.DB.prepare("SELECT * FROM location_colors").all();
                        const colorMap = (colors || []).reduce((acc: any, curr: any) => {
                            acc[curr.name] = curr.color;
                            return acc;
                        }, {});

                        colorMap['비상'] = 'hsl(0, 0%, 94%)';

                        // 5. Save Enriched Schedule to D1
                        await env.DB.prepare("INSERT OR REPLACE INTO schedules (username, date, data) VALUES (?, ?, ?)")
                            .bind(xcrewId, date, JSON.stringify(schedule))
                            .run();
                            
                        return Response.json({ success: true, data: schedule, colors: colorMap }, { headers: corsHeaders });
                    } catch (e: any) {
                        return Response.json({ success: false, error: e.message }, { status: 500, headers: corsHeaders });
                    }
                }
                
                if (path === "/api/xcrew/dia" && method === "GET") {
                    if (!session) return new Response("Unauthorized", { status: 401, headers: corsHeaders });
                    let targetUsername: string | null = null;
                    const requestedUsername = url.searchParams.get("username");

                    if (session.isAdmin) {
                        targetUsername = requestedUsername;
                    } else {
                        targetUsername = session.username;
                        if (requestedUsername && requestedUsername !== targetUsername) {
                            return new Response("Unauthorized: You can only access your own data.", { status: 403, headers: corsHeaders });
                        }
                    }

                    const date = url.searchParams.get("date");
                    if (!targetUsername || !date) return new Response("Missing params", { status: 400, headers: corsHeaders });

                    const row = await env.DB.prepare("SELECT data FROM dia_info WHERE username = ? AND date = ?")
                        .bind(targetUsername, date)
                        .first();
                    return Response.json({ success: true, data: row ? JSON.parse(row.data as string) : null }, { headers: corsHeaders });
                }

                if (path === "/api/xcrew/dia" && method === "POST") {
                    if (!session) return new Response("Unauthorized", { status: 401, headers: corsHeaders });
                    const { xcrewId, xcrewPw, date } = await request.json() as any;
                     if (!xcrewId || !xcrewPw || !date) {
                        return new Response("Missing params", { status: 400, headers: corsHeaders });
                    }

                    if (xcrewId !== session.username) {
                        return new Response("Unauthorized: You can only update your own dia.", { status: 403, headers: corsHeaders });
                    }
                    
                    const client = new KorailClient(xcrewId, xcrewPw);
                    try {
                        const info = await client.getDiaInfo(date);
                        // Save to D1
                        await env.DB.prepare("INSERT OR REPLACE INTO dia_info (username, date, data) VALUES (?, ?, ?)")
                            .bind(xcrewId, date, JSON.stringify(info))
                            .run();
                        return Response.json({ success: true, data: info }, { headers: corsHeaders });
                    } catch (e: any) {
                         return Response.json({ success: false, error: e.message }, { status: 500, headers: corsHeaders });
                    }
                }

                if (path === "/api/xcrew/work-pstt" && method === "POST") {
                    if (!session) return new Response("Unauthorized", { status: 401, headers: corsHeaders });
                    
                    const { username: requestedUsername, date, pdiaNo } = await request.json() as any;
                    let targetUsername: string;
                    
                    if (session.isAdmin) {
                        // Admins can query any user, or their own data if no username specified
                        targetUsername = requestedUsername || session.username;
                    } else {
                        // Non-admins can only access their own data
                        targetUsername = session.username;
                        if (requestedUsername && requestedUsername !== targetUsername) {
                            return new Response("Unauthorized: You can only access your own data.", { status: 403, headers: corsHeaders });
                        }
                    }

                    let query = "SELECT * FROM searchExtrCrewWrkPstt WHERE username = ?";
                    const params: any[] = [targetUsername];

                    if (date) {
                        query += " AND pjtDt = ?";
                        params.push(date);
                    }

                    if (pdiaNo) {
                        query += " AND pdiaNo = ?";
                        params.push(pdiaNo);
                    }

                    const { results } = await env.DB.prepare(query).bind(...params).all();

                    return Response.json({ 
                        success: true, 
                        data: results || []
                    }, { headers: corsHeaders });
                }
                
                if (path === "/api/train" && method === "POST") {
                     const { trainNo, driveDate } = await request.json() as any;
                     if (!trainNo || !driveDate) {
                         return new Response("Missing train info", { status: 400, headers: corsHeaders });
                     }
                     
                     const apiToken = await env.NXLOGIS_SECRET.get(); 
                     console.log(`[TrainAPI] Requesting for ${trainNo} on ${driveDate}. Token length: ${apiToken.length}`);
                     
                     const client = new TrainClient();
                     try {
                         const data = await client.getTrainData(trainNo, driveDate, apiToken);
                         return Response.json(data, { headers: corsHeaders });
                     } catch (e: any) {
                         return Response.json({ found: false, message: e.message }, { headers: corsHeaders });
                     }
                }

                if (path === "/api/push/register" && method === "POST") {
                    if (!session) return new Response("Unauthorized", { status: 401, headers: corsHeaders });
                    const { fcmToken, trainNo, driveDate, stationName, stationIndex } = await request.json() as any;
                    if (!fcmToken || !trainNo || !driveDate || !stationName || !stationIndex) {
                        return new Response("Missing fields: fcmToken, trainNo, driveDate, stationName, stationIndex are required", { status: 400, headers: corsHeaders });
                    }

                    const id = crypto.randomUUID();
                    try {
                        await env.DB.prepare(
                            "INSERT INTO push_notifications (id, username, fcmToken, trainNo, driveDate, stationName, stationIndex) VALUES (?, ?, ?, ?, ?, ?, ?)"
                        )
                            .bind(id, session.username, fcmToken, trainNo, driveDate, stationName, stationIndex)
                            .run();
                        return Response.json({ success: true, id }, { headers: corsHeaders });
                    } catch (e: any) {
                        return new Response(`Failed to register push notification: ${e.message}`, { status: 500, headers: corsHeaders });
                    }
                }

                if (path === "/api/push/unregister" && method === "POST") {
                    if (!session) return new Response("Unauthorized", { status: 401, headers: corsHeaders });
                    const { id, trainNo, driveDate, stationName } = await request.json() as any;
                    if (!id) {
                        return new Response("Missing id", { status: 400, headers: corsHeaders });
                    }

                    try {
                        var current_pushes = await env.DB.prepare("SELECT * FROM push_notifications WHERE id = ? AND username = ? AND trainNo = ? AND driveDate = ? AND stationName = ?")
                            .bind(id, session.username, trainNo, driveDate, stationName)
                            .first();
                    } catch (e: any) {
                        return new Response(`Push notification not found: ${e.message}`, { status: 404, headers: corsHeaders });
                    }

                    if (!current_pushes) {
                        return new Response("Push notification not found or does not belong to you", { status: 404, headers: corsHeaders });
                    }

                    try {
                        await env.DB.prepare("DELETE FROM push_notifications WHERE id = ? AND username = ?")
                            .bind(id, session.username)
                            .run();
                        return Response.json({ success: true }, { headers: corsHeaders });
                    } catch (e: any) {
                        return new Response(`Failed to unregister push notification: ${e.message}`, { status: 500, headers: corsHeaders });
                    }
                }

                if (path === "/api/push/list" && method === "GET") {                   
                    if (!session) return new Response("Unauthorized", { status: 401, headers: corsHeaders });
                    try {
                        const { results } = await env.DB.prepare("SELECT * FROM push_notifications WHERE username = ?")
                            .bind(session.username)
                            .all();
                        return Response.json({ success: true, data: results }, { headers: corsHeaders });
                    } catch (e: any) {
                        return new Response(`Failed to list push notifications: ${e.message}`, { status: 500, headers: corsHeaders });
                    }
                }

                // --- Admin Data Endpoints ---
                
                if (path === "/api/admin/users" && method === "GET") {
                    const { results } = await env.DB.prepare("SELECT id, username, name, created_at FROM users ORDER BY created_at DESC").all();
                    return Response.json({ success: true, data: results }, { headers: corsHeaders });
                }

                if (path === "/api/admin/users" && method === "POST") {
                    const { username, name } = await request.json() as any;
                    // search for users and return user data

                    let query = "SELECT id, username, name, created_at FROM users WHERE 1=1";
                    const params: any[] = [];

                    if (username) {
                        query += " AND username LIKE ?";
                        params.push(`%${username}%`);
                    }
                    if (name) {
                        query += " AND name LIKE ?";
                        params.push(`%${name}%`);
                    }

                    query += " ORDER BY created_at DESC";

                    const { results } = await env.DB.prepare(query).bind(...params).all();
                    return Response.json({ success: true, data: results }, { headers: corsHeaders });
                }

                if (path.startsWith("/api/admin/user/") && path.endsWith("/schedule") && method === "GET") {
                    // /api/admin/user/:username/schedule
                    const parts = path.split("/");
                    const targetUsername = parts[4];
                    const date = url.searchParams.get("date");

                    if (!targetUsername || !date) return new Response("Missing params", { status: 400, headers: corsHeaders });

                    // Reuse logic from /api/xcrew/schedule
                    const row = await env.DB.prepare("SELECT data FROM schedules WHERE username = ? AND date = ?")
                        .bind(targetUsername, date)
                        .first();
                        
                    let scheduleData = row ? JSON.parse(row.data as string) : null;

                    if (scheduleData && Array.isArray(scheduleData)) {
                        const monthPrefix = date.substring(0, 6);
                        const { results: locs } = await env.DB.prepare("SELECT date, location FROM working_locations WHERE username = ? AND date LIKE ?")
                            .bind(targetUsername, `${monthPrefix}%`)
                            .all();
                        
                        const locMap = (locs || []).reduce((acc: any, curr: any) => {
                            acc[curr.date] = curr.location;
                            return acc;
                        }, {});

                        scheduleData = scheduleData.map((item: any) => {
                            if (locMap[item.pjtDt]) {
                                item.location = locMap[item.pjtDt];
                            }
                            return item;
                        });
                    }

                    const { results: colors } = await env.DB.prepare("SELECT * FROM location_colors").all();
                    const colorMap = (colors || []).reduce((acc: any, curr: any) => {
                        acc[curr.name] = curr.color;
                        return acc;
                    }, {});
                    colorMap['비상'] = 'hsl(0, 0%, 94%)';

                    return Response.json({ 
                        success: true, 
                        data: scheduleData,
                        colors: colorMap
                    }, { headers: corsHeaders });
                }

                if (path.startsWith("/api/admin/user/") && path.endsWith("/profile") && method === "GET") {
                    const parts = path.split("/");
                    const targetUsername = parts[4];
                    const user = await env.DB.prepare("SELECT username, name FROM users WHERE username = ?").bind(targetUsername).first();
                    if (!user) return new Response("User not found", { status: 404, headers: corsHeaders });
                    return Response.json({ success: true, data: user }, { headers: corsHeaders });
                }

                if (path.startsWith("/api/admin/user/") && path.endsWith("/update-profile") && method === "POST") {
                    const parts = path.split("/");
                    const targetUsername = parts[4];
                    const { name } = await request.json() as any;
                    
                    if (!targetUsername || typeof name !== 'string') return new Response("Invalid params", { status: 400, headers: corsHeaders });

                    await env.DB.prepare("UPDATE users SET name = ? WHERE username = ?")
                        .bind(name, targetUsername)
                        .run();
                    
                    return Response.json({ success: true, message: "User profile updated" }, { headers: corsHeaders });
                }

                if (path.startsWith("/api/admin/user/") && path.endsWith("/reset-password") && method === "POST") {
                    const parts = path.split("/");
                    const targetUsername = parts[4];
                    const { newPassword } = await request.json() as any;
                    
                    if (!targetUsername || !newPassword) return new Response("Missing params", { status: 400, headers: corsHeaders });

                    const newHashBuffer = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(newPassword));
                    const newHashHex = Array.from(new Uint8Array(newHashBuffer)).map(b => b.toString(16).padStart(2, '0')).join('');

                    await env.DB.prepare("UPDATE users SET password_hash = ? WHERE username = ?")
                        .bind(newHashHex, targetUsername)
                        .run();
                    
                    return Response.json({ success: true, message: "User password reset successfully" }, { headers: corsHeaders });
                }

                if (path.startsWith("/api/admin/user/") && path.endsWith("/delete") && method === "DELETE") {
                    const parts = path.split("/");
                    const targetUsername = parts[4];
                    
                    if (!targetUsername) return new Response("Missing username", { status: 400, headers: corsHeaders });

                    await deleteUserAccount(env.DB, env.KORAIL_XCREW_SESSION_KV, targetUsername);

                    return Response.json({ success: true, message: "User deleted successfully" }, { headers: corsHeaders });
                }

                if (path.startsWith("/api/admin/user/") && path.endsWith("/dia") && method === "GET") {
                    // /api/admin/user/:username/dia
                    const parts = path.split("/");
                    const targetUsername = parts[4];
                    const date = url.searchParams.get("date");
                    
                    if (!targetUsername || !date) return new Response("Missing params", { status: 400, headers: corsHeaders });

                    const row = await env.DB.prepare("SELECT data FROM dia_info WHERE username = ? AND date = ?")
                        .bind(targetUsername, date)
                        .first();
                    return Response.json({ success: true, data: row ? JSON.parse(row.data as string) : null }, { headers: corsHeaders });
                }

            } catch (e: any) {
                return new Response(`Server Error: ${e.message}`, { status: 500, headers: corsHeaders });
            }
		}

		return new Response("Not Found", { status: 404 });
	},
} satisfies ExportedHandler<Env, WorkPsttQueueMessage>;

