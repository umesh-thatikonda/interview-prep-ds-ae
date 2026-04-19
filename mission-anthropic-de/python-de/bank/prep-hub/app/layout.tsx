import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Mission Anthropic | Prep Hub",
  description: "The ultimate Data Engineering interview preparation hub.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
