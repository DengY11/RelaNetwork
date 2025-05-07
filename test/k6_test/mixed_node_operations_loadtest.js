import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend } from 'k6/metrics';

// --- 配置 ---
const BASE_URL = 'http://localhost:8888'; // !! 修改为你的服务实际地址和端口 !!
const API_V1_PREFIX = '/api/v1';
const NUM_INITIAL_NODES = 30; // 在 setup 阶段创建的初始节点数量
const NODE_TYPE_PERSON = 1; // 假设 PERSON 节点的整数类型是 1

// --- 自定义指标 ---
const createNodeDuration = new Trend('http_req_duration_createnode', true);
const getNodeDuration = new Trend('http_req_duration_getnode', true);
const updateNodeDuration = new Trend('http_req_duration_updatenode', true);

// --- 测试阶段配置 ---
export const options = {
  thresholds: {
    'http_req_failed': ['rate<0.05'], // 整体失败率 < 5%
    'checks': ['rate>0.95'],          // 整体检查成功率 > 95%

    'http_req_duration_createnode': ['p(95)<1500'],
    'checks{name:CreateNodeChecks}': ['rate>0.90'],

    'http_req_duration_getnode': ['p(95)<500'],
    'checks{name:GetNodeChecks}': ['rate>0.95'],

    'http_req_duration_updatenode': ['p(95)<1200'],
    'checks{name:UpdateNodeChecks}': ['rate>0.90'],
  },
  scenarios: {
    mixed_node_ops: {
      executor: 'ramping-vus',
      stages: [
        { duration: '30s', target: 2000 }, 
        { duration: '1m', target: 2000 },
        { duration: '20s', target: 5500 },
        { duration: '1m', target: 5500 },
        { duration: '20s', target: 10000 },
        { duration: '1m', target: 10000 },
        { duration: '30s', target: 0 },
      ],
      gracefulRampDown: '30s',
    },
  },
};

// --- Helper Function: 创建节点 (用于 Setup 和 VU 阶段) ---
function createNode(namePrefix, vuId = 'N/A', iter = 'N/A', type = NODE_TYPE_PERSON) {
  const uniqueName = `${namePrefix}-VU${vuId}-Iter${iter}-${Date.now()}`;
  const payload = JSON.stringify({
    type: type,
    name: uniqueName,
    properties: { source: 'k6-mixed-ops', vu: `${vuId}`, iter: `${iter}` }
  });
  const params = { headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' } };
  return http.post(`${BASE_URL}${API_V1_PREFIX}/nodes`, payload, params);
}

// --- Setup Function: 创建初始测试节点 ---
export function setup() {
  console.log(`压测准备 (Mixed Node Ops): 正在创建 ${NUM_INITIAL_NODES} 个初始测试节点...`);
  const initialNodeIds = [];
  for (let i = 0; i < NUM_INITIAL_NODES; i++) {
    const res = createNode('InitialNode', 'Setup', `${i}`);
    if (res.status === 200 && res.json()?.success && res.json()?.node?.id) {
      initialNodeIds.push(res.json().node.id);
    } else {
      console.error(`  - Setup: 创建初始节点 ${i + 1} 失败! Status: ${res.status}, Body: ${res.body}`);
      // 可选择中止测试: throw new Error("Failed to create initial nodes in setup");
    }
    sleep(0.05); // 轻微延迟
  }
  if (initialNodeIds.length === 0) {
      console.error("压测准备 (Mixed Node Ops): 未能成功创建任何初始节点，测试可能不准确。");
      return { initialNodeIds: [], error: "No initial nodes created" };
  }
  console.log(`压测准备 (Mixed Node Ops): ${initialNodeIds.length} 个初始节点创建完成。`);
  return { initialNodeIds: initialNodeIds };
}

// --- Default Function: 混合节点操作 ---
export default function (data) {
  if (!data || !data.initialNodeIds || data.error) {
    console.error(`VU ${__VU} Iter ${__ITER}: Setup 数据无效或Setup失败 (${data.error || '未知错误'})，跳过。`);
    check(false, { 'SetupDataValid': () => false });
    sleep(1);
    return;
  }

  const choice = Math.random();
  let targetNodeId; // 用于 Get 和 Update
  const locallyCreatedNodeIds = []; // 存储此 VU 在此迭代中创建的节点 ID

  // --- 1. CreateNode (例如 20% 的概率) ---
  if (choice < 0.20) { 
    const createRes = createNode('DynamicNode', __VU, __ITER);
    createNodeDuration.add(createRes.timings.duration);
    const createCheck = check(createRes, {
      'CreateNode 状态码是 200': (r) => r.status === 200,
      'CreateNode 返回 success 为 true': (r) => r.status === 200 && r.json()?.success === true,
      'CreateNode 返回有效 Node ID': (r) => r.status === 200 && r.json()?.node?.id != null,
    }, { name: 'CreateNodeChecks' });
    if (!createCheck) {
        console.warn(`VU ${__VU} Iter ${__ITER}: CreateNode 检查失败. Status: ${createRes.status}, Body: ${createRes.body?.substring(0,100)}`);
    }
    if (createRes.status === 200 && createRes.json()?.node?.id) {
        locallyCreatedNodeIds.push(createRes.json().node.id); // 保存新创建的ID
    }
  }
  // --- 2. GetNode (例如 50% 的概率) ---
  else if (choice < 0.70) { 
    if (data.initialNodeIds.length > 0 || locallyCreatedNodeIds.length > 0) {
        const allAvailableIds = data.initialNodeIds.concat(locallyCreatedNodeIds);
        targetNodeId = allAvailableIds[Math.floor(Math.random() * allAvailableIds.length)];
        
        const getRes = http.get(`${BASE_URL}${API_V1_PREFIX}/nodes/${targetNodeId}`, { tags: { name: 'GetNode' } });
        getNodeDuration.add(getRes.timings.duration);
        const getCheck = check(getRes, {
            'GetNode 状态码是 200': (r) => r.status === 200,
            'GetNode 返回 success 为 true': (r) => r.status === 200 && r.json()?.success === true,
            'GetNode ID 匹配': (r) => r.status === 200 && r.json()?.node?.id === targetNodeId,
        }, { name: 'GetNodeChecks' });
        if (!getCheck) {
            console.warn(`VU ${__VU} Iter ${__ITER}: GetNode 检查失败. NodeID: ${targetNodeId}, Status: ${getRes.status}, Body: ${getRes.body?.substring(0,100)}`);
        }
    }
  }
  // --- 3. UpdateNode (例如 30% 的概率) ---
  else { 
    if (data.initialNodeIds.length > 0 || locallyCreatedNodeIds.length > 0) {
        const allAvailableIds = data.initialNodeIds.concat(locallyCreatedNodeIds);
        targetNodeId = allAvailableIds[Math.floor(Math.random() * allAvailableIds.length)];

        const newName = `UpdatedNode-VU${__VU}-Iter${__ITER}-${Date.now()}`;
        const updatePayload = JSON.stringify({
            name: newName,
            properties: { updated_by: `k6-vu${__VU}` }
        });
        const updateRes = http.put(`${BASE_URL}${API_V1_PREFIX}/nodes/${targetNodeId}`, updatePayload, {
            headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
            tags: { name: 'UpdateNode' }
        });
        updateNodeDuration.add(updateRes.timings.duration);
        const updateCheck = check(updateRes, {
            'UpdateNode 状态码是 200': (r) => r.status === 200,
            'UpdateNode 返回 success 为 true': (r) => r.status === 200 && r.json()?.success === true,
            'UpdateNode 名称已更新': (r) => r.status === 200 && r.json()?.node?.name === newName,
        }, { name: 'UpdateNodeChecks' });
        if (!updateCheck) {
            console.warn(`VU ${__VU} Iter ${__ITER}: UpdateNode 检查失败. NodeID: ${targetNodeId}, Status: ${updateRes.status}, Body: ${updateRes.body?.substring(0,100)}`);
        }
    }
  }
  sleep(Math.random() * 1 + 0.5); // 0.5s to 1.5s sleep
}

// --- Teardown Function: 清理初始创建的测试数据 ---
export function teardown(data) {
  console.log(`压测清理 (Mixed Node Ops): 正在清理 Setup 创建的初始节点...`);
  let idsToClean = [];
  if (data && data.initialNodeIds && data.initialNodeIds.length > 0) {
      idsToClean = data.initialNodeIds;
  } else if (data && data.error && data.nodeIdsToClean && data.nodeIdsToClean.length > 0){
      // Handle case where setup failed mid-way but returned some IDs to clean
      idsToClean = data.nodeIdsToClean;
      console.warn(`  - Setup 可能未完全成功，尝试清理 ${idsToClean.length} 个已创建的初始节点。`);
  }

  if (idsToClean.length > 0) {
    console.log(`  - 正在删除 ${idsToClean.length} 个初始节点...`);
    idsToClean.forEach(nodeId => {
      const res = http.del(`${BASE_URL}${API_V1_PREFIX}/nodes/${nodeId}`);
      if (res.status !== 200) {
        console.warn(`  - Teardown: 删除初始节点 ${nodeId} 失败: Status ${res.status}`);
      }
    });
    console.log(`  - ${idsToClean.length} 个初始节点清理请求已发送。`);
  } else {
    console.log(`压测清理 (Mixed Node Ops): 无需清理初始节点。`);
  }
  console.log(`压测清理 (Mixed Node Ops) 完成。`);
} 