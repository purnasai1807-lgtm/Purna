"use client";

import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useState,
  type PropsWithChildren
} from "react";

import { ApiError, getCurrentUser, login, loginPublicUser as requestPublicLogin, signup } from "@/lib/api";
import type { AuthMode, LoginPayload, SignupPayload, User } from "@/lib/types";

type AuthContextValue = {
  user: User | null;
  token: string | null;
  authMode: AuthMode | null;
  isLoading: boolean;
  loginUser: (payload: LoginPayload) => Promise<void>;
  signupUser: (payload: SignupPayload) => Promise<void>;
  loginPublicUser: () => Promise<void>;
  logoutUser: () => void;
};

const STORAGE_KEY = "auto-analytics-ai-auth";
const PUBLIC_AUTH_MODE: AuthMode = "public";
const ACCOUNT_AUTH_MODE: AuthMode = "account";

const AuthContext = createContext<AuthContextValue | undefined>(undefined);

export function AuthProvider({ children }: PropsWithChildren) {
  const [user, setUser] = useState<User | null>(null);
  const [token, setToken] = useState<string | null>(null);
  const [authMode, setAuthMode] = useState<AuthMode | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  function clearAuthState() {
    setToken(null);
    setUser(null);
    setAuthMode(null);
    window.localStorage.removeItem(STORAGE_KEY);
  }

  function persistAuthState(nextToken: string, nextMode: AuthMode) {
    setToken(nextToken);
    setAuthMode(nextMode);
    window.localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ token: nextToken, mode: nextMode })
    );
  }

  async function restorePublicUser() {
    const response = await requestPublicLogin();
    persistAuthState(response.access_token, PUBLIC_AUTH_MODE);
    setUser(response.user);
  }

  useEffect(() => {
    const rawState = window.localStorage.getItem(STORAGE_KEY);
    if (!rawState) {
      setIsLoading(false);
      return;
    }

    try {
      const parsed = JSON.parse(rawState) as { token?: string; mode?: AuthMode };
      const storedMode = parsed.mode === PUBLIC_AUTH_MODE ? PUBLIC_AUTH_MODE : ACCOUNT_AUTH_MODE;
      if (!parsed.token) {
        if (storedMode === PUBLIC_AUTH_MODE) {
          restorePublicUser()
            .catch(() => {
              clearAuthState();
            })
            .finally(() => setIsLoading(false));
          return;
        }

        clearAuthState();
        setIsLoading(false);
        return;
      }

      setToken(parsed.token);
      setAuthMode(storedMode);

      getCurrentUser(parsed.token)
        .then((currentUser) => {
          setUser(currentUser);
        })
        .catch(async (error) => {
          if (!(error instanceof ApiError) || ![401, 403].includes(error.status ?? 0)) {
            return;
          }

          if (storedMode === PUBLIC_AUTH_MODE) {
            try {
              await restorePublicUser();
            } catch {
              clearAuthState();
            }
            return;
          }

          clearAuthState();
        })
        .finally(() => setIsLoading(false));
    } catch {
      clearAuthState();
      setIsLoading(false);
    }
  }, []);

  async function loginUser(payload: LoginPayload) {
    const response = await login(payload);
    setUser(response.user);
    persistAuthState(response.access_token, ACCOUNT_AUTH_MODE);
  }

  async function signupUser(payload: SignupPayload) {
    const response = await signup(payload);
    setUser(response.user);
    persistAuthState(response.access_token, ACCOUNT_AUTH_MODE);
  }

  async function loginPublicUser() {
    await restorePublicUser();
  }

  function logoutUser() {
    clearAuthState();
  }

  const value = useMemo(
    () => ({ user, token, authMode, isLoading, loginUser, signupUser, loginPublicUser, logoutUser }),
    [user, token, authMode, isLoading]
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used inside an AuthProvider.");
  }
  return context;
}
