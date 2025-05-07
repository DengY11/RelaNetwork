import http from 'k6/http';
import { check, sleep } from 'k6';
// SharedArray is not needed here if setup populates a simple array for VUs
// import { SharedArray } from 'k6/data'; 
import { Trend } from 'k6/metrics';

// --- 配置 ---
const BASE_URL = 'http://localhost:8888'; // !! 修改为你的服务实际地址和端口 !!
const API_V1_PREFIX = '/api/v1';
const NUM_NODES_TO_CREATE_IN_SETUP = 50; // 在 setup 阶段创建的节点数量

// --- 自定义指标 ---
const getNodeDuration = new Trend('http_req_duration_getnode', true);

// --- 测试阶段 --- 
export const options = {
  thresholds: {
    'http_req_failed{name:GetNode}': ['rate<0.05'],       // GetNode 失败率 < 5%
    'checks{name:GetNodeChecks}': ['rate>0.95'],           // GetNode 检查成功率 > 95%
    'http_req_duration_getnode': ['p(95)<500', 'p(99)<800'], // GetNode p95 < 500ms, p99 < 800ms
  },
  scenarios: {
    default: {
      executor: 'ramping-vus',
      stages: [
        { duration: '20s', target: 1000 }, 
        { duration: '30s', target: 1000 },
        { duration: '10s', target: 3000 },
        { duration: '30s', target: 3000 },
        { duration: '10s', target: 5000 }, 
        { duration: '30s', target: 5000 },
        { duration: '20s', target: 0 },
      ],
      gracefulRampDown: '20s',
    },
  },
};

// --- Helper Function: 创建节点 (复用并简化) ---
function createNodeForSetup(nodeType, namePrefix) {
  const uniqueName = `${namePrefix}-SetupNode-${Date.now()}-${Math.random().toString(36).substring(2,7)}`;
  const payload = JSON.stringify({
    type: nodeType, // 使用整数类型
    name: uniqueName,
    properties: { source: 'k6-setup-getnode-test' }
  });
  const params = { headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' } };
  return http.post(`${BASE_URL}${API_V1_PREFIX}/nodes`, payload, params);
}

// --- Setup Function: 创建测试节点 (在 VU 执行前运行一次) ---
export function setup() {
    console.log(`压测准备 (GetNode): 正在创建 ${NUM_NODES_TO_CREATE_IN_SETUP} 个测试节点...`);
    const ids = [];
    // NodeType 使用整数值，假设 PERSON = 1
    const nodeTypePerson = 1; 

    for (let i = 0; i < NUM_NODES_TO_CREATE_IN_SETUP; i++) {
        const res = createNodeForSetup(nodeTypePerson, `GetNodeTest`);
        if (res.status === 200 && res.json() && res.json().success && res.json().node && res.json().node.id) {
            ids.push(res.json().node.id);
            if ((i + 1) % 10 === 0 || i === NUM_NODES_TO_CREATE_IN_SETUP - 1) {
                 console.log(`  - 已创建 ${ids.length}/${NUM_NODES_TO_CREATE_IN_SETUP} 个节点... (最后一个ID: ${ids[ids.length-1]})`);
            }
        } else {
            console.error(`  - 创建节点 ${i + 1} 失败! Status: ${res.status}, Body: ${res.body}`);
            // 如果一个节点创建失败，setup 将会中止，并返回已创建的 ID 以便清理
            // 或者你可以选择抛出错误来完全停止测试: throw new Error("Setup failed to create all nodes");
            return { error: `Failed to create node ${i+1}`, nodeIdsToClean: ids }; 
        }
        sleep(0.05); // 轻微延迟避免瞬间过大压力到 CreateNode
    }
    if (ids.length < NUM_NODES_TO_CREATE_IN_SETUP) {
        console.error(`压测准备 (GetNode): 未能成功创建所有 ${NUM_NODES_TO_CREATE_IN_SETUP} 个测试节点，只创建了 ${ids.length} 个。`);
        // 根据需要决定是否继续测试
        // return { error: "Not all nodes created in setup", nodeIdsToClean: ids };
    }
    console.log(`压测准备 (GetNode): ${ids.length} 个测试节点创建完成。`);
    return { nodeIds: ids }; // 将节点 ID 传递给 VU 和 teardown
}

// --- Default Function: GetNode 负载测试场景 ---
export default function (data) { // data 参数会接收 setup() 的返回值
  if (!data || !data.nodeIds || data.nodeIds.length === 0 || data.error) {
    console.error(`VU ${__VU} Iter ${__ITER}: Setup 未能提供节点 ID 或 Setup 失败 (${data.error || '未知错误'})，跳过本轮迭代。`);
    check(false, { 'SetupSucceededAndProvidedNodeIds': () => false });
    sleep(1);
    return;
  }

  // 从 setup 返回的数据中获取节点 ID 列表
  const nodeIdsFromSetup = data.nodeIds;

  // 从共享数组中随机选择一个 Node ID
  const nodeIdToGet = nodeIdsFromSetup[Math.floor(Math.random() * nodeIdsFromSetup.length)];

  const getNodeUrl = `${BASE_URL}${API_V1_PREFIX}/nodes/${nodeIdToGet}`;
  const getNodeParams = {
    headers: { 'Accept': 'application/json' },
    tags: { name: 'GetNode', operation: 'read' }, 
  };
  const getNodeRes = http.get(getNodeUrl, getNodeParams);

  // 添加到自定义趋势指标
  getNodeDuration.add(getNodeRes.timings.duration);

  // GetNode 检查
  const getNodeCheckRes = check(getNodeRes, {
    'GetNode 状态码是 200': (r) => r.status === 200,
    'GetNode 返回 success 为 true': (r) => r.status === 200 && r.json()?.success === true,
    'GetNode 返回的 Node ID 与请求一致': (r) => r.status === 200 && r.json()?.node?.id === nodeIdToGet,
    'GetNode 返回 Node 对象': (r) => r.status === 200 && typeof r.json()?.node === 'object' && r.json()?.node !== null,
  }, { name: 'GetNodeChecks' });

  if (!getNodeCheckRes) {
    console.warn(`VU ${__VU} Iter ${__ITER}: GetNode 请求检查失败。URL: ${getNodeUrl}, NodeID: ${nodeIdToGet}, 状态码: ${getNodeRes.status}, 响应体: ${getNodeRes.body?.substring(0,150)}...`);
  }

  sleep(Math.random() * 0.8 + 0.2); // 0.2 到 1.0 秒之间的随机思考时间
}

// --- Teardown Function: 清理测试数据 ---
export function teardown(data) { // data 参数会接收 setup() 的返回值
  console.log(`压测清理 (GetNode): 正在清理 Setup 创建的测试节点...`);
  let idsToClean = [];
  if (data && data.nodeIds && data.nodeIds.length > 0) {
      idsToClean = data.nodeIds;
  } else if (data && data.nodeIdsToClean && data.nodeIdsToClean.length > 0) {
      // 处理 setup 中途失败并返回 nodeIdsToClean 的情况
      idsToClean = data.nodeIdsToClean;
      console.warn(`  - Setup 可能未完全成功，尝试清理 ${idsToClean.length} 个已创建的节点。`);
  }

  if (idsToClean.length > 0) {
    console.log(`  - 正在删除 ${idsToClean.length} 个节点...`);
    idsToClean.forEach(nodeId => {
      const res = http.del(`${BASE_URL}${API_V1_PREFIX}/nodes/${nodeId}`);
      if (res.status !== 200) {
        // Teardown 中的失败通常只记录警告，不影响测试结果
        console.warn(`  - 删除节点 ${nodeId} 失败: Status ${res.status}, Body: ${res.body?.substring(0,100)}`);
      }
    });
    console.log(`  - ${idsToClean.length} 个节点清理请求已发送。`);
  } else {
    console.log(`压测清理 (GetNode): 无需清理 (没有节点ID被传递到 teardown 或 Setup 彻底失败未创建任何节点)。`);
  }
  console.log(`压测清理 (GetNode) 完成。`);
} 