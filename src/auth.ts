function base64UrlEncode(str: string | Uint8Array): string {
    const bytes = typeof str === 'string' ? new TextEncoder().encode(str) : str;
    return btoa(String.fromCharCode(...bytes))
        .replace(/\+/g, '-')
        .replace(/\//g, '_')
        .replace(/=+$/, '');
}

async function sign(data: string, secret: string): Promise<string> {
    const key = await crypto.subtle.importKey(
        'raw',
        new TextEncoder().encode(secret),
        { name: 'HMAC', hash: 'SHA-256' },
        false,
        ['sign']
    );
    const signature = await crypto.subtle.sign(
        'HMAC',
        key,
        new TextEncoder().encode(data)
    );
    return base64UrlEncode(new Uint8Array(signature));
}

export async function createJwt(payload: any, secret: string): Promise<string> {
    const header = JSON.stringify({ alg: 'HS256', typ: 'JWT' });
    const body = JSON.stringify({
        ...payload,
        iat: Math.floor(Date.now() / 1000),
        exp: Math.floor(Date.now() / 1000) + (60 * 60 * 24 * 365) // 365 days
    });

    const encodedHeader = base64UrlEncode(header);
    const encodedBody = base64UrlEncode(body);
    const data = `${encodedHeader}.${encodedBody}`;
    const signature = await sign(data, secret);

    return `${data}.${signature}`;
}

export async function createSession(kv: KVNamespace, username: string, secret: string, prefix: string = ""): Promise<string> {
    const jti = crypto.randomUUID();
    const token = await createJwt({ sub: username, role: prefix ? 'admin' : 'user', jti }, secret);
    await kv.put(`${prefix}${username}:${jti}`, token);
    return token;
}

export async function verifySession(kv: KVNamespace, request: Request, secret: string): Promise<{ username: string, isAdmin: boolean, jti?: string } | null> {
    let token: string | null = null;
    let isAdmin = false;

    // 1. Try to get token from cookies
    const cookieHeader = request.headers.get("Cookie");
    if (cookieHeader) {
        const cookies = cookieHeader.split(';').map(c => c.trim());
        
        // Check for admin token first
        const adminCookie = cookies.find(c => c.startsWith("admin_token="));
        if (adminCookie) {
            token = adminCookie.split("=")[1];
            isAdmin = true;
        } else {
            // Check for user token
            const userCookie = cookies.find(c => c.startsWith("auth_token="));
            if (userCookie) {
                token = userCookie.split("=")[1];
                isAdmin = false;
            }
        }
    }

    // 2. If no cookie, fall back to Authorization header
    if (!token) {
        const authHeader = request.headers.get("Authorization");
        if (authHeader && authHeader.startsWith("Bearer ")) {
            token = authHeader.split(" ")[1];
            // We need to decode the token to know if it's admin or user for the KV check
            // We'll do a preliminary decode here just to check the role claim if present, 
            // or we can rely on the signature verification and then check role.
            // Let's defer determining isAdmin for header auth until after signature check to be safe,
            // or decode the payload insecurely first (standard practice for routing) then verify.
            try {
                const parts = token.split('.');
                if (parts.length === 3) {
                    const payload = JSON.parse(atob(parts[1].replace(/-/g, '+').replace(/_/g, '/')));
                    isAdmin = payload.role === 'admin';
                }
            } catch (e) {
                return null;
            }
        }
    }

    if (!token) {
        return null; // No token found
    }
    
    // 3. Verify Signature
    try {
        const [headerB64, bodyB64, signatureB64] = token.split(".");
        if (!headerB64 || !bodyB64 || !signatureB64) {
            return null; // Invalid token format
        }
        
        const data = `${headerB64}.${bodyB64}`;
        const expectedSignature = await sign(data, secret);
        
        if (signatureB64 !== expectedSignature) {
            return null; // Invalid signature
        }
        
        // 4. Decode payload
        const payload = JSON.parse(atob(bodyB64.replace(/-/g, '+').replace(/_/g, '/')));
        const username = payload.sub;
        const jti = payload.jti;
        
        if (!username) return null;
        
        // Ensure the isAdmin flag matches the token claim (if we got token from cookie, we guessed based on cookie name)
        // If the token claims to be admin, we treat it as such for KV lookup.
        // If token from cookie said admin_token but payload says user, that's a mismatch, but we'll trust payload for KV lookup key.
        isAdmin = payload.role === 'admin';
        
        // 5. Verify against KV (Session Revocation Check)
        const prefix = isAdmin ? "admin:" : "";
        let storedToken: string | null = null;

        if (jti) {
             storedToken = await kv.get(`${prefix}${username}:${jti}`);
        } else {
            // Backward compatibility for old tokens without jti
             storedToken = await kv.get(`${prefix}${username}`);
        }
        
        if (storedToken !== token) {
            return null; // Token revoked or replaced
        }
        
        return { username, isAdmin, jti };
    } catch (e) {
        console.error("Token verification failed:", e);
        return null;
    }
}

export async function destroySession(kv: KVNamespace, username: string, jti?: string, prefix: string = "") {
    if (jti) {
        await kv.delete(`${prefix}${username}:${jti}`);
    } else {
        await kv.delete(`${prefix}${username}`);
    }
}

export async function revokeAllSessions(kv: KVNamespace, username: string, prefix: string = "") {
    // Delete all sessions for this user with jti
    const list = await kv.list({ prefix: `${prefix}${username}:` });
    for (const key of list.keys) {
        await kv.delete(key.name);
    }
    // Also delete old style key
    await kv.delete(`${prefix}${username}`);
}

export async function deleteUserAccount(db: D1Database, kv: KVNamespace, username: string, prefix: string = "") {
    const batch = [
                        db.prepare("DELETE FROM users WHERE username = ?").bind(username),
                        db.prepare("DELETE FROM schedules WHERE username = ?").bind(username),
                        db.prepare("DELETE FROM dia_info WHERE username = ?").bind(username),
                        db.prepare("DELETE FROM working_locations WHERE username = ?").bind(username)
                    ];
    
    await db.batch(batch);
    await revokeAllSessions(kv, username, prefix);
}