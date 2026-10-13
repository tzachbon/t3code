import * as NodeAssert from "node:assert/strict";
import { computeMicroUsd, monthWindow } from "./Workspaces.ts";

NodeAssert.equal(computeMicroUsd(4, 1_920_000), 384_000);
NodeAssert.equal(computeMicroUsd(1, 1), 1);
NodeAssert.deepEqual(monthWindow(Date.parse("2026-12-31T23:59:59Z")), [
  Date.parse("2026-12-01T00:00:00Z"),
  Date.parse("2027-01-01T00:00:00Z"),
]);
console.log("Depot integer billing and UTC month checks passed.");
