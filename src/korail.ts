import { extractCookiesFromResponse, mergeCookies } from './utils';

const BASE_URL = "https://xcrew.korail.com";
const LOGIN_VIEW_URL = `${BASE_URL}/loginView.do`;
const LOGIN_URL = `${BASE_URL}/login.do`;
const LOGOUT_URL = `${BASE_URL}/logout.do`;
const SCHEDULE_URL = `${BASE_URL}/extrCrewMg/searchExtrIndCrewWrk.do`;
const TRAIN_API_URL = "https://kodeholic.me/sjang/entrypoint.php";

const DEFAULT_HEADERS = {
    "Host": "xcrew.korail.com",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:146.0) Gecko/20100101 Firefox/146.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Priority": "u=0, i",
};

const AJAX_HEADERS = {
    "Host": "xcrew.korail.com",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:146.0) Gecko/20100101 Firefox/146.0",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": BASE_URL,
    "Connection": "keep-alive",
    "Priority": "u=0",
};

export class KorailClient {
    private employeeId: string;
    private password: string;
    private cookieHeader: string = "";
    private authenticated: boolean = false;

    constructor(employeeId: string, password: string) {
        this.employeeId = employeeId;
        this.password = password;
    }

    private updateCookies(res: Response) {
        const newCookies = extractCookiesFromResponse(res);
        this.cookieHeader = mergeCookies(this.cookieHeader, newCookies);
    }

    async authenticate(): Promise<boolean> {
        this.cookieHeader = ""; // Reset
        this.authenticated = false;

        // Step 1: Visit login page
        try {
            const res1 = await fetch(LOGIN_VIEW_URL, {
                headers: DEFAULT_HEADERS,
                redirect: 'manual'
            });
            this.updateCookies(res1);
        } catch (e) {
            console.error("Login page visit failed", e);
            throw new Error("Failed to connect to server");
        }

        // Step 2: Submit login
        const loginData = new URLSearchParams({
            "message": "",
            "epno": this.employeeId,
            "pwd": this.password,
        });

        const headers = {
            ...DEFAULT_HEADERS,
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": BASE_URL,
            "Referer": LOGIN_VIEW_URL,
            "Cookie": this.cookieHeader
        };

        try {
            const res2 = await fetch(LOGIN_URL, {
                method: 'POST',
                headers: headers,
                body: loginData,
                redirect: 'manual'
            });
            this.updateCookies(res2);

            if (res2.status === 302 || res2.status === 303) {
                const location = res2.headers.get("Location") || "";
                if (location.includes("extrRltmCrewWrkPstt.do")) {
                    // Success, follow redirect to confirm session
                     const redirectUrl = location.startsWith("http") ? location : `${BASE_URL}${location}`;
                     const res3 = await fetch(redirectUrl, {
                         headers: { ...DEFAULT_HEADERS, "Referer": LOGIN_URL, "Cookie": this.cookieHeader },
                         redirect: 'manual'
                     });
                     this.updateCookies(res3);
                     
                     if (res3.status === 302 || res3.status === 303) {
                         const loc3 = res3.headers.get("Location") || "";
                         if (loc3.includes("loginView.do")) {
                             throw new Error("Session not established");
                         }
                     }
                     this.authenticated = true;
                     return true;
                }
                if (location.includes("loginView.do")) {
                    throw new Error("Login failed: incorrect credentials");
                }
            }
             // Fallback check body
             const text = await res2.text();
             if (text.includes("loginView") || text.includes("로그인")) {
                 throw new Error("Login failed: incorrect credentials");
             }
             
             // If we got here, maybe 200 OK but on wrong page?
             throw new Error(`Unexpected login response: ${res2.status}`);

        } catch (e: any) {
             throw new Error(`Auth error: ${e.message}`);
        }
    }
    
    // We assume the caller wants data for a specific month or date
    // Python: get_schedule(target_date) where target_date is YYYYMMDD
    async getSchedule(targetDate: string, employeeName: string): Promise<any[]> {
        return this._execute(
            async () => {
                // Step 1: Visit list page
                const pageUrl = `${BASE_URL}/extrCrewMg/extrIndCrewWrkList.do`;
                const pageHeaders = {
                    ...DEFAULT_HEADERS,
                    "Referer": `${BASE_URL}/extrCrewMg/extrRltmCrewWrkPstt.do`,
                    "Cookie": this.cookieHeader
                };
                
                const res1 = await fetch(pageUrl, { headers: pageHeaders, redirect: 'manual' });
                this.updateCookies(res1);
                if (this._isSessionExpired(res1)) throw new Error("Session expired");

                // Step 2: AJAX request
                const headers = {
                    ...AJAX_HEADERS,
                    "Referer": pageUrl,
                    "Cookie": this.cookieHeader
                };
                const body = new URLSearchParams({
                    "empNm": employeeName,
                    "pjtDt": targetDate
                });

                const res2 = await fetch(SCHEDULE_URL, {
                    method: 'POST',
                    headers: headers,
                    body: body
                });
                this.updateCookies(res2);
                if (this._isSessionExpired(res2)) throw new Error("Session expired");

                const data = await this._parseJsonResponse(res2);
                return data.data || [];
            }
        );
    }

    private async _execute<T>(action: () => Promise<T>, retry: boolean = true): Promise<T> {
        if (!this.authenticated) {
            await this.authenticate();
        }

        try {
            return await action();
        } catch (e: any) {
            if (e.message.includes("Session expired") && retry) {
                console.log("Session expired, re-authenticating...");
                this.authenticated = false; // Force re-auth
                await this.authenticate();
                // Retry the action one more time
                return await this._execute(action, false);
            }
            // Re-throw if not a session error or if retry failed
            throw e;
        }
    }

    private _isSessionExpired(res: Response): boolean {
        if (res.status === 302 || res.status === 303) {
            const loc = res.headers.get("Location") || "";
            return loc.includes("loginView.do");
        }
        return false;
    }

    // New method to safely parse JSON or throw session error
    private async _parseJsonResponse(res: Response): Promise<any> {
        const text = await res.text();
        if (text.includes("loginView.do")) { // Check for HTML login page
            throw new Error("Session expired");
        }
        try {
            return JSON.parse(text);
        } catch (e) {
            console.error("Failed to parse JSON response:", text);
            throw new Error("Invalid JSON response from server");
        }
    }
    
    // Porting fetch_dia_info logic
    async getDiaInfo(date: string, knownPdiaNo?: string): Promise<any | null> {
        return this._execute(async () => {
            let pdiaNo = knownPdiaNo || "";

            if (!pdiaNo) {
                // Step 1: Get pdiaNo (Only if not provided)
                const pdiaUrl = `${BASE_URL}/extrCrewMg/searchPdiaNo.do`;
                const headers = {
                    ...AJAX_HEADERS,
                    "Referer": `${BASE_URL}/extrCrewMg/extrRltmCrewWrkPstt.do`,
                    "Cookie": this.cookieHeader
                };
                
                const res1 = await fetch(pdiaUrl, {
                    method: 'POST',
                    headers: headers,
                    body: new URLSearchParams({ "pdiaNo": "", "pjtDt": date })
                });
                
                if (this._isSessionExpired(res1)) throw new Error("Session expired");
                
                this.updateCookies(res1);
                
                const data1 = await this._parseJsonResponse(res1);
                if (data1.pdiaNo && Array.isArray(data1.pdiaNo) && data1.pdiaNo.length > 0) {
                    pdiaNo = data1.pdiaNo[0].pdiaNo;
                } else if (data1.extrCrewMgVO && data1.extrCrewMgVO.pdiaNo) {
                    pdiaNo = data1.extrCrewMgVO.pdiaNo;
                }
            }
            
            if (!pdiaNo) return null;
            
            // Step 2: Get Details
            const diaUrl = `${BASE_URL}/extrCrewMg/searchExtrCrewDia.do`;
            const headers2 = {
                ...AJAX_HEADERS,
                "Referer": `${BASE_URL}/extrCrewMg/extrRltmCrewWrkPstt.do`,
                "Cookie": this.cookieHeader
            };

            const res2 = await fetch(diaUrl, {
                method: 'POST',
                headers: headers2,
                body: new URLSearchParams({ "pdiaNo": pdiaNo, "pjtDt": date })
            });
            
            this.updateCookies(res2);
            
            if (this._isSessionExpired(res2)) throw new Error("Session expired");
            
            return await this._parseJsonResponse(res2);
        });
    }
}

// Train API Client (Kodeholic)
export class TrainClient {
    // This API seems to require a token? Python code has CONF_TRAIN_API_TOKEN.
    // We should probably ask the user for this or it might be hardcoded/shared.
    // The python code initializes TrainApi with auth_token.
    // For now, I'll allow passing it.
    
    constructor() {}

    async getTrainData(trainNo: string, driveDate: string, token: string): Promise<any> {
        // Simple proxy
        const res = await fetch(TRAIN_API_URL, {
            method: 'POST',
            headers: {
                "Content-Type": "application/json",
                "X-Auth-Token": token,
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:146.0) Gecko/20100101 Firefox/146.0"
            },
            body: JSON.stringify({
                "trainNo": trainNo,
                "driveDate": driveDate
            })
        });
        
        if (res.status === 401) throw new Error("Invalid train API token");
        if (!res.ok) throw new Error(`Train API error: ${res.status}`);
        
        const data = await res.json() as any;
        
        // --- KST Timezone Conversion ---
        const toKST = (utc: number | null | undefined): string | null => {
            if (utc === null || typeof utc === 'undefined') return null;
            // Create a Date object from the UTC timestamp (assuming it's in seconds)
            const date = new Date(utc * 1000);
            
            // Format to 'HH:mm:ss' in KST
            return date.toLocaleTimeString('en-GB', { timeZone: 'Asia/Seoul', hour12: false });
        };

        // Transform raw response to match expected structure
        if (data.result === 200 && data.data && data.data.info) {
            const schedule = (data.data.schedule || []).map((item: any) => ({
                ...item,
                scheduledArrivalTime: toKST(item.scheduledArrivalTime),
                scheduledDepartureTime: toKST(item.scheduledDepartureTime),
                actualArrivalTime: toKST(item.actualArrivalTime),
                actualDepartureTime: toKST(item.actualDepartureTime),
            }));

            return {
                found: true,
                message: "OK",
                info: data.data.info,
                schedule: schedule
            };
        } else {
            return {
                found: false,
                message: data.message || "No data found",
                raw: data
            };
        }
    }
}
