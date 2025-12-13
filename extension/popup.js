async function fetchSignals() {
  // آدرس API خود را جایگزین کنید
  const url = "https://YOUR_SERVER/analyze";
  // اینجا نمونه تست: می‌توانید payload را از storage یا صفحه وب بگیرید
  const payload = await chrome.storage?.local.get("payload");
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type":"application/json" },
    body: JSON.stringify(payload?.payload || {})
  });
  const data = await res.json();
  renderList("long", data.long_top);
  renderList("short", data.short_top);
}

function renderList(id, list) {
  const el = document.getElementById(id);
  el.innerHTML = "";
  list.forEach(item => {
    const row = document.createElement("div");
    row.className = "row";
    row.textContent = `${item.symbol} | ${item.regime} | score: ${item.score.toFixed(3)}`;
    el.appendChild(row);
  });
}

fetchSignals().catch(err => {
  document.getElementById("long").textContent = "خطا در دریافت داده";
  document.getElementById("short").textContent = "";
});
