"use client";

import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useState,
  type PropsWithChildren
} from "react";

import { getCurrentUser, login, signup } from "@/lib/api";
import type { LoginPayload, SignupPayload, User } from "@/lib/types";

type AuthContextValue = {
  user: User | null;
  token: string | null;
  isLoading: boolean;
  loginUser: (payload: LoginPayload) => Promise<void>;
  signupUser: (payload: SignupPayload) => Promise<void>;
  logoutUser: () => void;
};

const STORAGE_KEY = "auto-analytics-ai-auth";

const AuthContext = createContext<AuthContextValue | undefined>(undefined);

export function AuthProvider({ children }: PropsWithChildren) {
  const [user, setUser] = useState<User | null>(null);
  const [token, setToken] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    const rawState = window.localStorage.getItem(STORAGE_KEY);
    if (!rawState) {
      setIsLoading(false);
      return;
    }

    try {
      const parsed = JSON.parse(rawState) as { token: string };
      if (!parsed.token) {
        setIsLoading(false);
        return;
      }

      getCurrentUser(parsed.token)
        .then((currentUser) => {
          setToken(parsed.token);
          setUser(currentUser);
        })
        .catch(() => {
          window.localStorage.removeItem(STORAGE_KEY);
        })
        .finally(() => setIsLoading(false));
    } catch {
      window.localStorage.removeItem(STORAGE_KEY);
      setIsLoading(false);
    }
  }, []);

  async function loginUser(payload: LoginPayload) {
    const response = await login(payload);
    setToken(response.access_token);
    setUser(response.user);
    window.localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ token: response.access_token })
    );
  }

  async function signupUser(payload: SignupPayload) {
    const response = await signup(payload);
    setToken(response.access_token);
    setUser(response.user);
    window.localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ token: response.access_token })
    );
  }

  function logoutUser() {
    setToken(null);
    setUser(null);
    window.localStorage.removeItem(STORAGE_KEY);
  }

  const value = useMemo(
    () => ({ user, token, isLoading, loginUser, signupUser, logoutUser }),
    [user, token, isLoading]
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
