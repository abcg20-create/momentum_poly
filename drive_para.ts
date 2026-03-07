import fs from "fs";
import path from "path";
import { createSign } from "crypto";

export type ParaBucket = "project" | "area" | "resource" | "archive";

const BUCKET_TO_FOLDER: Record<ParaBucket, string> = {
  project: "P_Projects",
  area: "A_Areas",
  resource: "R_Resources",
  archive: "X_Archive",
};

type ServiceAccount = {
  type: string;
  project_id?: string;
  private_key_id?: string;
  private_key: string;
  client_email: string;
  client_id?: string;
  token_uri?: string;
  universe_domain?: string;
};

function base64Url(input: string | Buffer): string {
  return Buffer.from(input)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function resolveCredentialsPath(): string {
  const envPath = String(process.env.GOOGLE_APPLICATION_CREDENTIALS || "").trim();
  if (envPath) return envPath;
  return path.resolve(process.cwd(), "gdrive-service-account.json");
}

function loadServiceAccount(): ServiceAccount {
  const credsPath = resolveCredentialsPath();
  if (!fs.existsSync(credsPath)) {
    throw new Error(`Service account JSON not found at GOOGLE_APPLICATION_CREDENTIALS (${credsPath})`);
  }
  const raw = fs.readFileSync(credsPath, "utf8");
  const parsed = JSON.parse(raw) as ServiceAccount;
  if (parsed?.type !== "service_account") {
    throw new Error("GOOGLE_APPLICATION_CREDENTIALS is not a service_account JSON");
  }
  if (!parsed?.client_email || !parsed?.private_key) {
    throw new Error("Service account JSON is missing client_email/private_key");
  }
  return parsed;
}

async function getAccessToken(scopes: string[]): Promise<string> {
  const sa = loadServiceAccount();
  const nowSec = Math.floor(Date.now() / 1000);
  const tokenUri = String(sa.token_uri || "https://oauth2.googleapis.com/token");
  const header = { alg: "RS256", typ: "JWT", kid: sa.private_key_id || undefined };
  const payload = {
    iss: sa.client_email,
    scope: scopes.join(" "),
    aud: tokenUri,
    iat: nowSec,
    exp: nowSec + 3600,
  };
  const unsigned = `${base64Url(JSON.stringify(header))}.${base64Url(JSON.stringify(payload))}`;
  const signer = createSign("RSA-SHA256");
  signer.update(unsigned);
  signer.end();
  const sig = signer.sign(sa.private_key);
  const assertion = `${unsigned}.${base64Url(sig)}`;

  const r = await fetch(tokenUri, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
      assertion,
    }),
  });
  if (!r.ok) {
    const body = await r.text().catch(() => "");
    throw new Error(`Token exchange failed (${r.status}): ${body.slice(0, 400)}`);
  }
  const data = (await r.json()) as any;
  const token = String(data?.access_token || "");
  if (!token) throw new Error("Token exchange succeeded but access_token missing");
  return token;
}

type DriveNode = {
  id: string;
  name: string;
  mimeType: string;
  webViewLink?: string;
  parents?: string[];
  modifiedTime?: string;
};

async function driveList(token: string, q: string): Promise<DriveNode[]> {
  const base = "https://www.googleapis.com/drive/v3/files";
  const u = new URL(base);
  u.searchParams.set("q", q);
  u.searchParams.set("includeItemsFromAllDrives", "true");
  u.searchParams.set("supportsAllDrives", "true");
  u.searchParams.set("fields", "files(id,name,mimeType,webViewLink,parents,modifiedTime)");
  u.searchParams.set("pageSize", "200");
  u.searchParams.set("orderBy", "folder,name");

  const r = await fetch(u.toString(), {
    method: "GET",
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });
  if (!r.ok) {
    const body = await r.text().catch(() => "");
    throw new Error(`Drive list failed (${r.status}): ${body.slice(0, 400)}`);
  }
  const data = (await r.json()) as any;
  return Array.isArray(data?.files) ? (data.files as DriveNode[]) : [];
}

async function driveCreateFolder(token: string, parentId: string, name: string): Promise<DriveNode> {
  const u = new URL("https://www.googleapis.com/drive/v3/files");
  u.searchParams.set("supportsAllDrives", "true");
  u.searchParams.set("fields", "id,name,mimeType,webViewLink,parents,modifiedTime");
  const r = await fetch(u.toString(), {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      name,
      parents: [parentId],
      mimeType: "application/vnd.google-apps.folder",
    }),
  });
  if (!r.ok) {
    const body = await r.text().catch(() => "");
    throw new Error(`Drive create folder failed (${r.status}): ${body.slice(0, 400)}`);
  }
  return (await r.json()) as DriveNode;
}

async function driveGetFile(token: string, fileId: string): Promise<DriveNode & { trashed?: boolean }> {
  const u = new URL(`https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}`);
  u.searchParams.set("supportsAllDrives", "true");
  u.searchParams.set("fields", "id,name,mimeType,webViewLink,parents,modifiedTime,trashed");
  const r = await fetch(u.toString(), {
    method: "GET",
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!r.ok) {
    const body = await r.text().catch(() => "");
    throw new Error(`Drive get file failed (${r.status}): ${body.slice(0, 400)}`);
  }
  return (await r.json()) as DriveNode & { trashed?: boolean };
}

async function driveMoveFile(token: string, fileId: string, newParentId: string): Promise<DriveNode> {
  const existing = await driveGetFile(token, fileId);
  const removeParents = Array.isArray(existing.parents) ? existing.parents.join(",") : "";
  const u = new URL(`https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}`);
  u.searchParams.set("supportsAllDrives", "true");
  u.searchParams.set("addParents", newParentId);
  if (removeParents) u.searchParams.set("removeParents", removeParents);
  u.searchParams.set("fields", "id,name,mimeType,webViewLink,parents,modifiedTime");
  const r = await fetch(u.toString(), {
    method: "PATCH",
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!r.ok) {
    const body = await r.text().catch(() => "");
    throw new Error(`Drive move file failed (${r.status}): ${body.slice(0, 400)}`);
  }
  return (await r.json()) as DriveNode;
}

async function findSingleFolderByName(token: string, parentId: string, folderName: string): Promise<DriveNode | null> {
  const q = [
    `'${parentId}' in parents`,
    `mimeType='application/vnd.google-apps.folder'`,
    `trashed=false`,
    `name='${folderName.replace(/'/g, "\\'")}'`,
  ].join(" and ");
  const files = await driveList(token, q);
  return files.length ? files[0] : null;
}

async function findSingleNodeByName(token: string, parentId: string, name: string): Promise<DriveNode | null> {
  const q = [
    `'${parentId}' in parents`,
    `trashed=false`,
    `name='${name.replace(/'/g, "\\'")}'`,
  ].join(" and ");
  const files = await driveList(token, q);
  return files.length ? files[0] : null;
}

async function ensureFolderPath(token: string, parentId: string, segments: string[]): Promise<DriveNode> {
  let curParent = parentId;
  let last: DriveNode | null = null;
  for (const segRaw of segments) {
    const seg = String(segRaw || "").trim();
    if (!seg) continue;
    let folder = await findSingleFolderByName(token, curParent, seg);
    if (!folder) folder = await driveCreateFolder(token, curParent, seg);
    curParent = folder.id;
    last = folder;
  }
  if (!last) throw new Error("ensureFolderPath requires at least one non-empty path segment");
  return last;
}

function guessMimeTypeFromName(name: string): string {
  const lower = String(name || "").toLowerCase();
  if (lower.endsWith(".json") || lower.endsWith(".jsonl")) return "application/json";
  if (lower.endsWith(".txt") || lower.endsWith(".tsv") || lower.endsWith(".csv") || lower.endsWith(".log")) return "text/plain";
  if (lower.endsWith(".gz")) return "application/gzip";
  if (lower.endsWith(".parquet")) return "application/octet-stream";
  return "application/octet-stream";
}

async function driveUploadFileCreate(token: string, parentId: string, name: string, content: Buffer, mimeType: string): Promise<DriveNode> {
  const boundary = `-------codex-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const metadata = { name, parents: [parentId] };
  const preamble = `--${boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n${JSON.stringify(metadata)}\r\n`;
  const mediaHeader = `--${boundary}\r\nContent-Type: ${mimeType}\r\n\r\n`;
  const closing = `\r\n--${boundary}--`;
  const body = Buffer.concat([
    Buffer.from(preamble, "utf8"),
    Buffer.from(mediaHeader, "utf8"),
    content,
    Buffer.from(closing, "utf8"),
  ]);
  const u = new URL("https://www.googleapis.com/upload/drive/v3/files");
  u.searchParams.set("uploadType", "multipart");
  u.searchParams.set("supportsAllDrives", "true");
  u.searchParams.set("fields", "id,name,mimeType,webViewLink,parents,modifiedTime");
  const r = await fetch(u.toString(), {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": `multipart/related; boundary=${boundary}`,
    },
    body,
  });
  if (!r.ok) {
    const txt = await r.text().catch(() => "");
    throw new Error(`Drive file upload failed (${r.status}): ${txt.slice(0, 400)}`);
  }
  return (await r.json()) as DriveNode;
}

async function driveUploadFileUpdate(token: string, fileId: string, content: Buffer, mimeType: string): Promise<DriveNode> {
  const u = new URL(`https://www.googleapis.com/upload/drive/v3/files/${encodeURIComponent(fileId)}`);
  u.searchParams.set("uploadType", "media");
  u.searchParams.set("supportsAllDrives", "true");
  u.searchParams.set("fields", "id,name,mimeType,webViewLink,parents,modifiedTime");
  const r = await fetch(u.toString(), {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": mimeType,
    },
    body: content,
  });
  if (!r.ok) {
    const txt = await r.text().catch(() => "");
    throw new Error(`Drive file update failed (${r.status}): ${txt.slice(0, 400)}`);
  }
  return (await r.json()) as DriveNode;
}

export async function getParaTree(bucket?: ParaBucket) {
  const rootId = String(process.env.GDRIVE_PARA_ROOT_FOLDER_ID || "").trim();
  if (!rootId) {
    throw new Error("GDRIVE_PARA_ROOT_FOLDER_ID is not set");
  }
  const token = await getAccessToken(["https://www.googleapis.com/auth/drive.readonly"]);
  const buckets: ParaBucket[] = bucket ? [bucket] : ["project", "area", "resource", "archive"];
  const result: Record<string, any> = {};

  for (const b of buckets) {
    const folderName = BUCKET_TO_FOLDER[b];
    const bucketFolder = await findSingleFolderByName(token, rootId, folderName);
    if (!bucketFolder) {
      result[b] = { folderName, found: false, folderId: null, children: [] };
      continue;
    }
    const children = await driveList(
      token,
      [`'${bucketFolder.id}' in parents`, `trashed=false`].join(" and ")
    );
    result[b] = {
      folderName,
      found: true,
      folderId: bucketFolder.id,
      webViewLink: bucketFolder.webViewLink || null,
      children: children.map((c) => ({
        id: c.id,
        name: c.name,
        mimeType: c.mimeType,
        webViewLink: c.webViewLink || null,
        modifiedTime: c.modifiedTime || null,
      })),
    };
  }

  return {
    ok: true,
    rootFolderId: rootId,
    asOfMs: Date.now(),
    buckets: result,
  };
}

export async function bootstrapParaFolders() {
  const rootId = String(process.env.GDRIVE_PARA_ROOT_FOLDER_ID || "").trim();
  if (!rootId) throw new Error("GDRIVE_PARA_ROOT_FOLDER_ID is not set");
  const token = await getAccessToken(["https://www.googleapis.com/auth/drive"]);
  const buckets: ParaBucket[] = ["project", "area", "resource", "archive"];
  const out: Record<string, any> = {};

  for (const b of buckets) {
    const folderName = BUCKET_TO_FOLDER[b];
    let folder = await findSingleFolderByName(token, rootId, folderName);
    let created = false;
    if (!folder) {
      folder = await driveCreateFolder(token, rootId, folderName);
      created = true;
    }
    out[b] = {
      folderName,
      folderId: folder.id,
      webViewLink: folder.webViewLink || null,
      created,
    };
  }

  return {
    ok: true,
    rootFolderId: rootId,
    asOfMs: Date.now(),
    buckets: out,
  };
}

export async function moveFileToParaBucket(input: {
  fileId: string;
  bucket: ParaBucket;
  targetFolderId?: string | null;
}) {
  const rootId = String(process.env.GDRIVE_PARA_ROOT_FOLDER_ID || "").trim();
  if (!rootId) throw new Error("GDRIVE_PARA_ROOT_FOLDER_ID is not set");
  const fileId = String(input.fileId || "").trim();
  if (!fileId) throw new Error("fileId is required");
  const token = await getAccessToken(["https://www.googleapis.com/auth/drive"]);
  const bucketFolderName = BUCKET_TO_FOLDER[input.bucket];
  const bucketFolder = await findSingleFolderByName(token, rootId, bucketFolderName);
  if (!bucketFolder) {
    throw new Error(`Bucket folder ${bucketFolderName} not found. Call /api/v2/drive/para/bootstrap first.`);
  }
  const targetFolderId = String(input.targetFolderId || "").trim() || bucketFolder.id;
  const moved = await driveMoveFile(token, fileId, targetFolderId);
  return {
    ok: true,
    asOfMs: Date.now(),
    bucket: input.bucket,
    bucketFolderId: bucketFolder.id,
    targetFolderId,
    file: moved,
  };
}

export async function archiveRunFolder(fileId: string) {
  return moveFileToParaBucket({
    fileId,
    bucket: "archive",
  });
}

export async function syncRunArtifactsToPara(input: {
  runId: number;
  localPaths: string[];
  areaFolderName?: string | null;
}) {
  const rootId = String(process.env.GDRIVE_PARA_ROOT_FOLDER_ID || "").trim();
  if (!rootId) throw new Error("GDRIVE_PARA_ROOT_FOLDER_ID is not set");
  const runId = Number(input.runId);
  if (!Number.isFinite(runId)) throw new Error("runId is required");
  const areaFolderName = String(input.areaFolderName || process.env.GDRIVE_RUN_AREA_FOLDER || "A_PaperTrading").trim() || "A_PaperTrading";
  const token = await getAccessToken(["https://www.googleapis.com/auth/drive"]);
  const areaRoot = await findSingleFolderByName(token, rootId, BUCKET_TO_FOLDER.area);
  if (!areaRoot) {
    throw new Error("A_Areas folder missing under PARA root. Call /api/v2/drive/para/bootstrap first.");
  }
  const runFolder = await ensureFolderPath(token, areaRoot.id, [areaFolderName, "runs", `run_${Math.floor(runId)}`]);
  const paths = Array.from(new Set((input.localPaths || []).map((p) => String(p || "").trim()).filter((p) => !!p)));
  const uploaded: Array<{
    localPath: string;
    name: string;
    fileId: string;
    webViewLink: string | null;
    bytes: number;
    updatedExisting: boolean;
  }> = [];
  const skipped: Array<{ localPath: string; reason: string }> = [];

  for (const localPath of paths) {
    if (!fs.existsSync(localPath)) {
      skipped.push({ localPath, reason: "missing" });
      continue;
    }
    const st = fs.statSync(localPath);
    if (!st.isFile()) {
      skipped.push({ localPath, reason: "not_file" });
      continue;
    }
    const content = fs.readFileSync(localPath);
    const name = path.basename(localPath);
    const mimeType = guessMimeTypeFromName(name);
    const existing = await findSingleNodeByName(token, runFolder.id, name);
    const node = existing
      ? await driveUploadFileUpdate(token, existing.id, content, mimeType)
      : await driveUploadFileCreate(token, runFolder.id, name, content, mimeType);
    uploaded.push({
      localPath,
      name,
      fileId: node.id,
      webViewLink: node.webViewLink || null,
      bytes: content.length,
      updatedExisting: !!existing,
    });
  }

  return {
    ok: true,
    asOfMs: Date.now(),
    runId: Math.floor(runId),
    paraBucket: "area",
    areaFolderName,
    runFolderId: runFolder.id,
    runFolderLink: runFolder.webViewLink || null,
    uploaded,
    skipped,
  };
}
