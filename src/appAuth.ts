const APP_AUTH_PREFIX = "appauth";
const NONCE_TTL_SECONDS = 10 * 60;
const CLOCK_SKEW_MS = 5 * 60 * 1000;

function base64UrlEncodeBytes(bytes: Uint8Array): string {
    return btoa(String.fromCharCode(...bytes))
        .replace(/\+/g, "-")
        .replace(/\//g, "_")
        .replace(/=+$/, "");
}

function base64UrlDecodeToBytes(input: string): Uint8Array {
    const padded = input.replace(/-/g, "+").replace(/_/g, "/").padEnd(Math.ceil(input.length / 4) * 4, "=");
    const binary = atob(padded);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
        bytes[i] = binary.charCodeAt(i);
    }
    return bytes;
}

async function sha256Base64Url(buffer: ArrayBuffer): Promise<string> {
    const digest = await crypto.subtle.digest("SHA-256", buffer);
    return base64UrlEncodeBytes(new Uint8Array(digest));
}

function appPublicKeyKey(username: string, deviceId: string): string {
    return `${APP_AUTH_PREFIX}:pubkey:${username}:${deviceId}`;
}

function nonceKey(username: string, deviceId: string, nonce: string): string {
    return `${APP_AUTH_PREFIX}:nonce:${username}:${deviceId}:${nonce}`;
}

export function isValidP256PublicKey(base64Url: string): boolean {
    try {
        const bytes = base64UrlDecodeToBytes(base64Url);
        return bytes.length === 65 && bytes[0] === 0x04;
    } catch {
        return false;
    }
}

export async function verifyAppRequest(
    env: Env,
    request: Request,
    username: string
): Promise<{ ok: boolean; status: number; message: string }> {
    const headers = request.headers;
    const deviceId = headers.get("X-App-Id");
    const timestamp = headers.get("X-App-Timestamp");
    const nonce = headers.get("X-App-Nonce");
    const signature = headers.get("X-App-Signature");

    if (!deviceId || !timestamp || !nonce || !signature) {
        return { ok: false, status: 401, message: "Missing app authentication headers" };
    }

    const ts = Number(timestamp);
    if (!Number.isFinite(ts)) {
        return { ok: false, status: 400, message: "Invalid app timestamp" };
    }

    const now = Date.now();
    if (Math.abs(now - ts) > CLOCK_SKEW_MS) {
        return { ok: false, status: 401, message: "App timestamp out of range" };
    }

    const publicKeyBase64 = await env.APP_AUTH_KV.get(appPublicKeyKey(username, deviceId));
    if (!publicKeyBase64) {
        return { ok: false, status: 401, message: "App key not registered" };
    }

    const nonceRecordKey = nonceKey(username, deviceId, nonce);
    const used = await env.APP_AUTH_KV.get(nonceRecordKey);
    if (used) {
        return { ok: false, status: 401, message: "Replay detected" };
    }

    const url = new URL(request.url);
    const bodyHash = await sha256Base64Url(await request.clone().arrayBuffer());
    const canonical = [
        request.method.toUpperCase(),
        `${url.pathname}${url.search}`,
        timestamp,
        nonce,
        bodyHash
    ].join("\n");

    try {
        const publicKeyBytes = base64UrlDecodeToBytes(publicKeyBase64);
        const signatureBytes = base64UrlDecodeToBytes(signature);
        const key = await crypto.subtle.importKey(
            "raw",
            publicKeyBytes,
            { name: "ECDSA", namedCurve: "P-256" },
            false,
            ["verify"]
        );
        const ok = await crypto.subtle.verify(
            { name: "ECDSA", hash: "SHA-256" },
            key,
            signatureBytes,
            new TextEncoder().encode(canonical)
        );
        if (!ok) {
            return { ok: false, status: 401, message: "Invalid app signature" };
        }
    } catch {
        return { ok: false, status: 401, message: "Invalid app signature" };
    }

    await env.APP_AUTH_KV.put(nonceRecordKey, "1", { expirationTtl: NONCE_TTL_SECONDS });
    return { ok: true, status: 200, message: "ok" };
}

export const appAuthKeys = {
    appPublicKeyKey
};
