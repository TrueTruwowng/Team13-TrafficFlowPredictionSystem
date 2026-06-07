export function getApiBaseUrl(): string {
  const envBase = typeof process !== "undefined" ? process.env.NEXT_PUBLIC_API_BASE_URL : undefined;
  if (envBase && envBase.length > 0) return envBase.replace(/\/+$/g, "");

  if (typeof window !== "undefined") {
    const h = window.location.hostname;
    return `http://${h}:8001`;
  }

  // Server-side fallback
  return "http://localhost:8001";
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