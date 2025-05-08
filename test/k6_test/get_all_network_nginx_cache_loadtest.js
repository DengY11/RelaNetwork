import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';

// --- 配置 ---
// !! 重要: 修改为 Nginx 代理的地址和端口 !!
const NGINX_BASE_URL = 'http://localhost:8083';
const API_ENDPOINT = '/api/v1/network'; // API路径，不带任何查询参数

// --- 自定义指标 ---
const getAllNetworkDuration = new Trend('http_req_duration_get_all_network', true);
const cacheHitRate = new Rate('nginx_cache_hit_rate'); // 自定义比率来估算缓存命中

// --- 测试阶段配置 ---
export const options = {
  thresholds: {
    'http_req_failed{scenario:default}': ['rate<0.02'], // 整体失败率应较低
    'checks{scenario:default}': ['rate>0.98'],          // 检查成功率
    'http_req_duration_get_all_network': ['p(95)<200', 'p(99)<500'], // 期望缓存命中后响应非常快
    'nginx_cache_hit_rate': ['rate>0.9'], // 期望大部分请求在缓存预热后命中缓存 (估算)
  },
  scenarios: {
    default: { // 单个场景，持续施压
      executor: 'ramping-vus',
      startVUs: 1,
      stages: [
        { duration: '1m', target: 1000 },   // 缓慢增加VU，让缓存有机会预热
        { duration: '3m', target: 10000 },  // 增加负载，观察缓存效果
        { duration: '1m', target: 10000 },  // 保持中等负载
        { duration: '30s', target: 0 },   // 逐渐减少VU
      ],
      gracefulRampDown: '30s',
    },
  },
};

// --- VU 默认执行函数 ---
export default function () {
  const requestUrl = `${NGINX_BASE_URL}${API_ENDPOINT}`; // 不带任何查询参数

  const res = http.get(requestUrl, {
    headers: { 'Accept': 'application/json' },
    tags: { name: 'GetAllNetworkViaNginx' },
  });

  getAllNetworkDuration.add(res.timings.duration);

  let responseJson = null;
  let isCacheHit = false; // 标志位，用于估算缓存命中

  try {
    responseJson = res.json();
  } catch (e) {
    console.error(`VU ${__VU} Iter ${__ITER}: Failed to parse JSON response. Status: ${res.status}, Body: ${res.body}`);
  }

  // 从 Nginx 添加的响应头中获取缓存状态
  const cacheStatus = res.headers['X-Proxy-Cache']; // Nginx 配置中添加的头部
  if (cacheStatus === 'HIT') {
    isCacheHit = true;
  }
  cacheHitRate.add(isCacheHit); // 如果是 HIT，则为 true (1)，否则为 false (0)

  const checksPassed = check(res, {
    'Status code is 200': (r) => r.status === 200,
    'Response contains success field (if JSON)': (r) => r.status !== 200 || (responseJson && typeof responseJson.success !== 'undefined'),
    // 'Response success is true (if JSON and success field exists)': (r) => r.status !== 200 || !responseJson || typeof responseJson.success === 'undefined' || responseJson.success === true,
    // 可选：检查响应体中是否有 nodes 和 relations 数组，但不检查具体数量
    'Response has nodes array (if JSON and success)': (r) => r.status !== 200 || !responseJson || !responseJson.success || Array.isArray(responseJson.nodes),
    'Response has relations array (if JSON and success)': (r) => r.status !== 200 || !responseJson || !responseJson.success || Array.isArray(responseJson.relations),
  });

  if (!checksPassed) {
    // console.log(`VU ${__VU} Iter ${__ITER}: Request to ${requestUrl} failed. Status: ${res.status}. Cache-Status: ${cacheStatus}. Body: ${res.body}`);
  } else if (isCacheHit) {
    // console.log(`VU ${__VU} Iter ${__ITER}: Cache HIT for ${requestUrl}. Duration: ${res.timings.duration}ms`);
  } else if (cacheStatus) { // MISS, BYPASS, EXPIRED etc.
    // console.log(`VU ${__VU} Iter ${__ITER}: Cache ${cacheStatus} for ${requestUrl}. Duration: ${res.timings.duration}ms`);
  }


  // 随机休眠1到3秒，模拟真实用户行为
  sleep(Math.random() * 2 + 1);
}

// --- Teardown 阶段 (可选) ---
// export function teardown(data) {
//   console.log('Teardown phase complete.');
// } 
