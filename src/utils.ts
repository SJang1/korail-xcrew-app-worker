// Helper to extract cookies from a Response object (using getSetCookie if available)
export function parseSetCookie(header: string | null): Record<string, string> {
  if (!header) return {};
  const cookies: Record<string, string> = {};
  
  const parts = header.split(';');
  const [nameValue] = parts;
  const [name, ...valueParts] = nameValue.split('=');
  if (name && valueParts.length > 0) {
      cookies[name.trim()] = valueParts.join('=').trim();
  }
  return cookies;
}

export function mergeCookies(existing: string, newCookies: Record<string, string>): string {
  const cookieMap = new Map<string, string>();
  if (existing) {
      existing.split(';').forEach(c => {
          const [name, val] = c.split('=').map(s => s.trim());
          if (name) cookieMap.set(name, val || '');
      });
  }
  
  Object.entries(newCookies).forEach(([name, val]) => {
      cookieMap.set(name, val);
  });
  
  const parts: string[] = [];
  cookieMap.forEach((val, name) => {
      parts.push(`${name}=${val}`);
  });
  return parts.join('; ');
}

export function extractCookiesFromResponse(res: Response): Record<string, string> {
    const cookies: Record<string, string> = {};
    // @ts-ignore - getSetCookie is a newer API
    if (typeof res.headers.getSetCookie === 'function') {
        // @ts-ignore
        const setCookies = res.headers.getSetCookie();
        for (const sc of setCookies) {
            Object.assign(cookies, parseSetCookie(sc));
        }
    } else {
        const sc = res.headers.get('Set-Cookie');
        if (sc) {
             Object.assign(cookies, parseSetCookie(sc));
        }
    }
    return cookies;
}

// Simple concurrency limiter
export async function mapConcurrent<T, R>(
    items: T[], 
    concurrency: number, 
    fn: (item: T) => Promise<R>
): Promise<R[]> {
    const results: R[] = [];
    const iterator = items.entries();
    const workers = Array(concurrency).fill(iterator).map(async (it) => {
        for (const [_, item] of it) {
            results.push(await fn(item));
        }
    });
    await Promise.all(workers);
    return results;
}

// Color generator (Pastel)
export function generateColor(seed: string): string {
    let hash = 5381;
    for (let i = 0; i < seed.length; i++) {
        hash = ((hash << 5) + hash) + seed.charCodeAt(i);
    }
    
    // Ensure positive and spread hues better
    // Using a prime multiplier to increase distance between close hashes
    const h = Math.abs(hash * 131) % 360;
    
    // Vary lightness and saturation slightly for better distinction
    const s = 65 + (Math.abs(hash) % 15); // 65-80%
    const l = 80 + (Math.abs(hash) % 10); // 80-90%
    
    return `hsl(${h}, ${s}%, ${l}%)`;
}