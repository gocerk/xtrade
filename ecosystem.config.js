module.exports = {
  apps: [{
    name: "trader-bot",
    script: "server.js",
    autorestart: true,
    max_restarts: 10,
    restart_delay: 4000,
    watch: false,
    max_memory_restart: '6G',
    node_args: '--max-old-space-size=6144',
    env: {
      NODE_ENV: "production"
    },
    error_file: "logs/err.log",
    out_file: "logs/out.log",
    time: true,
    exp_backoff_restart_delay: 100,
    kill_timeout: 3000,
    cron_restart: "0 */6 * * *"  // Her 6 saatte bir yeniden başlatma
  }]
}