import type { Metadata } from "next";

import "./globals.css";
import TopNavbar from "../components/top-navbar";

export const metadata: Metadata = {
  title: "TrafficFlow Dashboard",
  description: "Next.js backbone for the TrafficFlow dashboard.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="vi">
      <body>
        <TopNavbar />
        {children}
      </body>
    </html>
  );
}