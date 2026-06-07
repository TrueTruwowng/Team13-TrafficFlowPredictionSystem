export function getApiBaseUrl(): string {
  const envBase = typeof process !== "undefined" ? process.env.NEXT_PUBLIC_API_BASE_URL : undefined;
  if (envBase && envBase.length > 0) return envBase.replace(/\/+$/g, "");

  if (typeof window !== "undefined") {
    const h = window.location.hostname;
    if (h === "localhost" || h === "127.0.0.1") return "http://localhost:8000";
    return "http://backend:8000"; // when running inside Docker compose
  }

  // Server-side fallback (assume compose network)
  return "http://backend:8000";
}

export async function fetchJson<T>(path: string): Promise<T> {
  const requestUrl = /^https?:\/\//i.test(path) ? path : `${getApiBaseUrl()}${path}`;
  const response = await fetch(requestUrl, {
    headers: {
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`${response.status} ${response.statusText}${body ? ` - ${body}` : ""}`);
  }

  return (await response.json()) as T;
}