import { WORKLOAD_MAX_LEN } from "./constants.js";
import { InvalidWorkloadError } from "./errors.js";

export function validateWorkload(workload: string): void {
  if (workload === "" || workload.length > WORKLOAD_MAX_LEN) {
    throw new InvalidWorkloadError();
  }
  for (let i = 0; i < workload.length; i += 1) {
    const ch = workload.charCodeAt(i);
    const ok =
      (ch >= 97 && ch <= 122) ||
      (ch >= 65 && ch <= 90) ||
      (ch >= 48 && ch <= 57) ||
      ch === 46 ||
      ch === 45 ||
      ch === 95 ||
      ch === 47;
    if (!ok) {
      throw new InvalidWorkloadError();
    }
  }
}
