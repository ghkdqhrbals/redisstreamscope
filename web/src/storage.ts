export function readMigratedStorage(
  storage: Storage,
  key: string,
  legacyKey: string,
): string | null {
  try {
    const current = storage.getItem(key);
    if (current !== null) return current;
    const legacy = storage.getItem(legacyKey);
    if (legacy === null) return null;
    storage.setItem(key, legacy);
    storage.removeItem(legacyKey);
    return legacy;
  } catch {
    return null;
  }
}
