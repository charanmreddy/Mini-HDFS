const $ = (id) => document.getElementById(id);
let selected = null;

const bytes = (n) => {
  const u = ["B", "KB", "MB", "GB"]; let i = 0;
  while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
  return (i === 0 ? n : n.toFixed(1)) + " " + u[i];
};
const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

async function refresh() {
  let s;
  try {
    const r = await fetch("/api/status");
    s = await r.json();
    if (!r.ok) throw new Error(s.error || "error");
  } catch (e) {
    $("summary").textContent = "namenode unreachable";
    return;
  }
  const nodes = Object.values(s.datanodes);
  const alive = nodes.filter((n) => n.alive).length;
  $("summary").innerHTML =
    `${alive}/${nodes.length} datanodes alive · ${s.total_files} file(s) · ${s.total_chunks} chunk(s) · ` +
    (s.under_replicated ? `<span class="warn">${s.under_replicated} chunk(s) under-replicated</span>` : `<span class="good">fully replicated</span>`);

  $("nodes").innerHTML = nodes.length ? nodes.map((n) => `
    <div class="node ${n.alive ? "" : "dead"}">
      <span class="pill">${n.alive ? "ALIVE" : "DEAD"}</span><b>${esc(n.id)}</b>
      <div>${esc(n.host)}:${n.chunk_port}</div>
      <div>${n.chunk_count} chunk(s) · ${bytes(n.storage_bytes)}</div>
      <div>last heartbeat ${n.age_sec}s ago</div>
    </div>`).join("") : `<div class="muted">No datanodes have registered yet.</div>`;

  const names = Object.keys(s.files).sort();
  $("empty").hidden = names.length > 0;
  $("files").querySelector("tbody").innerHTML = names.map((name) => {
    const f = s.files[name];
    const rep = f.status !== "COMPLETE" ? "-" : f.under_replicated
      ? `<span class="warn">${f.under_replicated} chunk(s) short</span>` : `<span class="good">${s.replication_factor} copies</span>`;
    return `<tr class="click" data-name="${esc(name)}">
      <td>${esc(name)}</td><td>${bytes(f.size)}</td><td>${f.num_chunks}</td>
      <td><span class="tag ${f.status}">${f.status}</span></td><td>${rep}</td>
      <td class="actions"><a href="/api/download/${encodeURIComponent(name)}"><button class="ghost">Download</button></a>
      <button class="ghost danger" data-del="${esc(name)}">Delete</button></td></tr>`;
  }).join("");

  if (selected && s.files[selected]) loadMap(selected, false);
  else if (selected) { selected = null; $("mapSection").hidden = true; }
}

async function loadMap(name, resetMsg = true) {
  selected = name;
  if (resetMsg) $("verifyMsg").textContent = "";
  const r = await fetch("/api/chunkmap/" + encodeURIComponent(name));
  if (!r.ok) return;
  const m = await r.json();
  $("mapSection").hidden = false;
  $("mapName").textContent = name + " (" + bytes(m.size) + ")";
  $("map").querySelector("thead").innerHTML =
    `<tr><th>#</th><th>Chunk</th><th>Size</th><th>Checksum</th>${m.datanodes.map((d) => `<th>${esc(d)}</th>`).join("")}<th><button class="ghost" id="verifyBtn">Verify integrity</button></th></tr>`;
  $("map").querySelector("tbody").innerHTML = m.chunks.map((c) => `
    <tr><td>${c.index}</td><td class="mono">${esc(c.chunk_id)}</td><td>${bytes(c.size)}</td>
    <td class="mono">${c.checksum ? c.checksum.slice(0, 10) + "…" : "-"}</td>
    ${m.datanodes.map((d) => {
      const st = c.replicas[d];
      return `<td class="${st === "alive" ? "dot-live" : st === "dead" ? "dot-dead" : "dot-none"}">${st ? "●" : "○"}</td>`;
    }).join("")}<td></td></tr>`).join("");
  $("verifyBtn").onclick = () => verify(name);
}

async function verify(name) {
  const box = $("verifyMsg");
  box.className = "msg"; box.textContent = "Reading every copy of every chunk…";
  const r = await fetch("/api/verify/" + encodeURIComponent(name));
  const v = await r.json();
  if (!r.ok) { box.className = "msg err"; box.textContent = v.error; return; }
  box.className = "msg " + (v.all_ok ? "ok" : "err");
  box.textContent = `${v.replicas_ok} of ${v.replicas_checked} copies match their checksum` + (v.all_ok ? " — file is intact." : " — some copies are bad or unreachable.");
}

function upload() {
  const file = $("file").files[0];
  const msg = $("uploadMsg");
  if (!file) { msg.className = "msg err"; msg.textContent = "Choose a file first."; return; }
  const fd = new FormData(); fd.append("file", file);
  const xhr = new XMLHttpRequest();
  $("uploadBtn").disabled = true; msg.className = "msg"; msg.textContent = "Uploading…";
  xhr.upload.onprogress = (e) => { if (e.lengthComputable) $("barFill").style.width = (e.loaded / e.total * 100) + "%"; };
  xhr.onload = () => {
    $("uploadBtn").disabled = false;
    let res = {}; try { res = JSON.parse(xhr.responseText); } catch (_) {}
    if (xhr.status === 200) {
      msg.className = "msg ok";
      msg.textContent = `Stored ${res.filename}: ${res.chunks} chunk(s), ${res.copies_stored} copies.` + (res.warnings.length ? " Warnings: " + res.warnings.join("; ") : "");
    } else { msg.className = "msg err"; msg.textContent = res.error || ("Upload failed: " + (res.status || xhr.status)); }
    $("barFill").style.width = "0"; refresh();
  };
  xhr.onerror = () => { $("uploadBtn").disabled = false; msg.className = "msg err"; msg.textContent = "Upload failed (network)."; };
  xhr.open("POST", "/api/upload"); xhr.send(fd);
}

document.addEventListener("click", async (e) => {
  const del = e.target.closest("[data-del]");
  if (del) { await fetch("/api/files/" + encodeURIComponent(del.dataset.del), { method: "DELETE" }); refresh(); return; }
  if (e.target.closest("a")) return;
  const row = e.target.closest("tr.click");
  if (row) loadMap(row.dataset.name);
});
$("uploadBtn").onclick = upload;
refresh(); setInterval(refresh, 2000);
