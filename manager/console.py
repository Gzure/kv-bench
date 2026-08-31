CONSOLE_HTML = r'''<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><title>kv-bench Manager</title>
<style>
body{font:14px system-ui;margin:0;background:#f4f6f8;color:#17202a}header{background:#14263d;color:white;padding:18px 28px}main{max-width:1200px;margin:22px auto;padding:0 18px}.grid{display:grid;grid-template-columns:1fr 1fr;gap:18px}.card{background:white;border:1px solid #dce3ea;border-radius:8px;padding:16px;box-shadow:0 2px 8px #0000000d}h2{margin-top:0;font-size:18px}button{background:#2563eb;color:white;border:0;border-radius:4px;padding:7px 11px;cursor:pointer;margin:3px}button.danger{background:#dc2626}input,textarea{width:100%;box-sizing:border-box;border:1px solid #cbd5e1;border-radius:4px;padding:8px;margin:4px 0 10px;font:13px monospace}textarea{height:115px}table{width:100%;border-collapse:collapse}td,th{text-align:left;border-bottom:1px solid #e5e7eb;padding:7px}.muted{color:#64748b}.metric{display:inline-block;background:#eef2ff;border-radius:5px;padding:10px;margin:4px}
</style></head><body><header><h1>kv-bench Manager Console</h1><span class="muted">节点、部署、拓扑任务与聚合结果</span></header><main><div class="grid">
<section class="card"><h2>节点管理</h2><div id="nodes"></div><textarea id="node" placeholder='{"name":"a","ip":"10.0.0.1","user":"root","password":"...","api_port":18082}'></textarea><button onclick="saveNode()">保存节点</button></section>
<section class="card"><h2>部署管理</h2><textarea id="deploy" placeholder='{"nodes":[...],"artifact":"build/kv-bench","destination":"/opt/kv-bench/build/kv-bench","source_dir":"/opt/kv-bench","umdk_root":"/opt/umdk"}'></textarea><button onclick="deploy()">部署并启动 worker</button><pre id="deployResult"></pre></section>
<section class="card"><h2>创建任务</h2><textarea id="task" placeholder='{"workers":[{"name":"a","ip":"10.0.0.1"},{"name":"b","ip":"10.0.0.2"}],"bench_items":[{"src":"a","dst":"b","type":"bidirectional"}],"options":{"op":"write","duration":30,"threads":4}}'></textarea><button onclick="createTask()">创建任务</button><pre id="taskResult"></pre></section>
<section class="card"><h2>任务与聚合结果</h2><div id="tasks"></div><div id="result"></div></section>
</div></main><script>
const api=(p,o)=>fetch(p,o).then(r=>r.json());
function showNodes(){api('/v1/nodes').then(xs=>{document.querySelector('#nodes').innerHTML='<table><tr><th>名称</th><th>地址</th><th>worker API</th><th></th></tr>'+xs.map(x=>`<tr><td>${x.name}</td><td>${x.ip}</td><td>${x.api_port}</td><td><button class="danger" onclick="delNode('${x.name}')">删除</button></td></tr>`).join('')+'</table>'})}
function saveNode(){api('/v1/nodes',{method:'POST',headers:{'Content-Type':'application/json'},body:document.querySelector('#node').value}).then(()=>{showNodes()})}
function delNode(n){api('/v1/nodes/'+encodeURIComponent(n),{method:'DELETE'}).then(showNodes)}
function deploy(){api('/v1/deploy',{method:'POST',headers:{'Content-Type':'application/json'},body:document.querySelector('#deploy').value}).then(x=>document.querySelector('#deployResult').textContent=JSON.stringify(x,null,2))}
function createTask(){api('/v1/tasks',{method:'POST',headers:{'Content-Type':'application/json'},body:document.querySelector('#task').value}).then(x=>{document.querySelector('#taskResult').textContent=JSON.stringify(x,null,2);showTasks()})}
function showTasks(){api('/v1/tasks').then(xs=>{document.querySelector('#tasks').innerHTML=xs.map(x=>`<p><b>${x.task_id}</b> ${x.state} <button onclick="startTask('${x.task_id}')">启动</button><button class="danger" onclick="stopTask('${x.task_id}')">停止</button><button onclick="result('${x.task_id}')">结果</button></p>`).join('')})}
function startTask(id){api('/v1/tasks/'+id+'/start',{method:'POST'}).then(showTasks)}
function stopTask(id){api('/v1/tasks/'+id+'/stop',{method:'POST'}).then(showTasks)}
function result(id){api('/v1/tasks/'+id+'/result').then(x=>{let a=x.aggregate||{};document.querySelector('#result').innerHTML=`<div class="metric">ops ${a.ops||0}</div><div class="metric">iops ${(a.iops||0).toFixed(2)}</div><div class="metric">bandwidth ${(a.bandwidth_mb_s||0).toFixed(2)} MB/s</div><div class="metric">errors ${a.errors||0}</div><pre>${JSON.stringify(x,null,2)}</pre>`})}
showNodes();showTasks();setInterval(showTasks,3000);
</script></body></html>'''
