export default function RunbookCard() {
  return (
    <div className="card">
      <h3>Runbook 小抄（查看/排查）</h3>
      <div className="rb-grid">
        <div>
          <strong>图例说明</strong>
          <ul>
            <li>近 7 日规则 KPI：近 7 天触发数、边际均值（按规则聚合）。</li>
            <li>Markets：市场列表与最近价（8s 刷新）。</li>
            <li>Signal Stream：规则信号流（5s 刷新），支持半自动下单。</li>
            <li>Health：DB 连通与规则心跳（5s 刷新）。</li>
          </ul>
        </div>
        <div>
          <strong>快速自检</strong>
          <ul>
            <li>容器状态：<code>docker compose ps</code></li>
            <li>API 健康：<code>curl http://localhost:8080/api/healthz</code></li>
            <li>信号列表：<code>curl http://localhost:8080/api/signals</code></li>
            <li>采集日志：<code>docker compose logs ingestor -f</code></li>
            <li>规则日志：<code>docker compose logs worker -f</code></li>
          </ul>
        </div>
        <div>
          <strong>常见问题</strong>
          <ul>
            <li>Signal 空/报错：看 worker 日志/health 是否 ok。</li>
            <li>Markets 不动：看 ingestor 日志与 /metrics 的 ingest 延迟。</li>
            <li>KPI 不更新：需先有信号触发，检查 <code>mpx_signals_total</code>。</li>
            <li>Telegram 未达：先 <code>POST /api/alerts/test</code> 验证；再查 <code>mpx_telegram_failures_total</code>。</li>
          </ul>
        </div>
      </div>
    </div>
  );
}

