"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { usePathname } from "next/navigation";

const NAV_ITEMS = [
  { href: "/", label: "Dữ liệu chính" },
  { href: "/visualization", label: "Trực quan hóa" },
];

export default function TopNavbar() {
  const pathname = usePathname();
  const [isDark, setIsDark] = useState(false);

  useEffect(() => {
    const stored = localStorage.getItem("tf_theme");
    const dark = stored
      ? stored === "dark"
      : window.matchMedia("(prefers-color-scheme: dark)").matches;
    setIsDark(dark);
    document.documentElement.setAttribute("data-theme", dark ? "dark" : "light");
  }, []);

  function toggleTheme() {
    const next = !isDark;
    setIsDark(next);
    document.documentElement.setAttribute("data-theme", next ? "dark" : "light");
    localStorage.setItem("tf_theme", next ? "dark" : "light");
  }

  return (
    <header className="top-navbar">
      <div className="top-navbar__inner">
        <div className="top-navbar__brand">
          <span className="top-navbar__eyebrow">TrafficFlow</span>
          <span className="top-navbar__title">Dashboard</span>
        </div>
        <div className="top-navbar__actions">
          <nav className="top-navbar__nav" aria-label="Điều hướng trang">
            {NAV_ITEMS.map((item) => {
              const isActive = item.href === "/" ? pathname === "/" : pathname.startsWith(item.href);
              return (
                <Link
                  key={item.href}
                  href={item.href}
                  className={`top-navbar__link ${isActive ? "is-active" : ""}`}
                  aria-current={isActive ? "page" : undefined}
                >
                  {item.label}
                </Link>
              );
            })}
          </nav>
          <button
            className="theme-toggle"
            onClick={toggleTheme}
            aria-label={isDark ? "Chuyển sang light mode" : "Chuyển sang dark mode"}
          >
            {isDark ? "☀" : "☽"}
          </button>
        </div>
      </div>
    </header>
  );
}
