"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

import { useAuth } from "@/components/providers/auth-provider";

export function SiteHeader() {
  const pathname = usePathname();
  const { user, logoutUser } = useAuth();

  return (
    <header className="site-header">
      <div className="shell site-header__inner">
        <Link href="/" className="brand">
          <span className="brand__badge">AI</span>
          <span>
            <strong>Auto Analytics AI</strong>
            <small>Upload data. Get decisions.</small>
          </span>
        </Link>

        <nav className="site-nav">
          <Link href="/" className={pathname === "/" ? "nav-link nav-link--active" : "nav-link"}>
            Home
          </Link>
          <Link
            href={user ? "/dashboard" : "/auth"}
            className={pathname?.startsWith("/dashboard") ? "nav-link nav-link--active" : "nav-link"}
          >
            Dashboard
          </Link>
          {user ? (
            <button type="button" className="button button--ghost" onClick={logoutUser}>
              Sign out
            </button>
          ) : (
            <Link href="/auth" className="button button--primary">
              Sign in
            </Link>
          )}
        </nav>
      </div>
    </header>
  );
}

