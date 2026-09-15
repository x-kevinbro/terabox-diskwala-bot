export function formatSize(bytes) {
  const n = Number(bytes) || 0;
  if (n >= 1024 * 1024 * 1024) return `${(n / 1024 / 1024 / 1024).toFixed(2)} GB`;
  if (n >= 1024 * 1024) return `${(n / 1024 / 1024).toFixed(2)} MB`;
  if (n >= 1024) return `${(n / 1024).toFixed(2)} KB`;
  return `${n} bytes`;
}

export function findBetween(str, start, end) {
  const startIndex = str.indexOf(start);
  if (startIndex === -1) return '';
  const endIndex = str.indexOf(end, startIndex + start.length);
  if (endIndex === -1) return '';
  return str.slice(startIndex + start.length, endIndex);
}
