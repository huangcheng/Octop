import { describe, expect, it } from "vitest";

import { filenameFromContentDisposition } from "./request";

describe("filenameFromContentDisposition", () => {
  it("prefers RFC 5987 filename*", () => {
    const header =
      "attachment; filename=\"download.zip\"; filename*=UTF-8''%E8%BF%90%E7%BB%B4%E5%8A%A9%E6%89%8B.zip";
    expect(filenameFromContentDisposition(header)).toBe("运维助手.zip");
  });

  it("reads quoted ascii filename", () => {
    expect(
      filenameFromContentDisposition('attachment; filename="pdf-reader.zip"'),
    ).toBe("pdf-reader.zip");
  });
});
