"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const NAV_ITEMS = [
  { href: "/", label: "Dữ liệu chính" },
  { href: "/visualization", label: "Trực quan hóa" },
];

export default function TopNavbar() {
  const pathname = usePathname();

  return (
    <header className="top-navbar">
      <div className="top-navbar__inner">
        <div className="top-navbar__brand">
          <span className="top-navbar__eyebrow">TrafficFlow</span>
          <span className="top-navbar__title">Dashboard</span>
        </div>
        <nav className="top-navbar__nav" aria-label="Điều hướng trang">
          {NAV_ITEMS.map((item) => {
            const isActive = item.href === "/" ? pathname === "/" : pathname.startsWith(item.href);
            return (
              <Link key={item.href} href={item.href} className={`top-navbar__link ${isActive ? "is-active" : ""}`} aria-current={isActive ? "page" : undefined}>
                {item.label}
              </Link>
            );
          })}
        </nav>
      </div>
    </header>
  );
}
