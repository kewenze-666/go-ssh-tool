package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
)

// Result 定义结果结构体
type Result struct {
	Host     string
	Output   string
	Err      error
	Duration time.Duration
}

// ExportResult 专门用于 JSON 导出的结构体
type ExportResult struct {
	Host     string `json:"host"`
	Output   string `json:"output"`
	Success  bool   `json:"success"`
	ErrorMsg string `json:"error,omitempty"`
	Duration string `json:"duration"`
}

// execOnHost 执行单台机器的核心函数
func execOnHost(ctx context.Context, host, user, cmd string, resultChan chan<- Result) {
	start := time.Now()

	key, err := os.ReadFile(`C:\Users\71845\.ssh\id_ed25519`)
	if err != nil {
		resultChan <- Result{Host: host, Err: fmt.Errorf("读私钥失败: %w", err), Duration: time.Since(start)}
		return
	}

	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		resultChan <- Result{Host: host, Err: fmt.Errorf("解析私钥失败: %w", err), Duration: time.Since(start)}
		return
	}

	config := &ssh.ClientConfig{
		User:            user,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         3 * time.Second,
	}

	client, err := ssh.Dial("tcp", host, config)
	if err != nil {
		resultChan <- Result{Host: host, Err: fmt.Errorf("连接失败: %w", err), Duration: time.Since(start)}
		return
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		resultChan <- Result{Host: host, Err: fmt.Errorf("开 session 失败: %w", err), Duration: time.Since(start)}
		return
	}
	defer session.Close()

	// 🔥 监控对讲机：如果主程序喊停（超时了），就强杀这个连接
	go func() {
		<-ctx.Done() // 阻塞在这里，直到收到取消信号
		if ctx.Err() == context.DeadlineExceeded {
			// 发送 SIGKILL 信号，直接杀掉远程进程
			session.Signal(ssh.SIGKILL)
			session.Close()
		}
	}()

	output, err := session.Output(cmd)
	duration := time.Since(start)

	if err == nil {
		slog.Info("命令执行完毕", "host", host, "耗时", duration.String())
	}

	resultChan <- Result{
		Host:     host,
		Output:   string(output),
		Err:      err,
		Duration: duration,
	}
}

func main() {
	data, err := os.ReadFile("hosts.txt")
	if err != nil {
		slog.Error("读取 hosts.txt 失败", "error", err)
		return
	}

	hosts := strings.Split(strings.TrimSpace(string(data)), "\n")
	slog.Info("成功加载了主机，准备并发执行...", "count", len(hosts))

	// 🔥 把这里改成 sleep 10，测试超时熔断！
	cmd := "sleep 10 && echo done"
	results := make(chan Result, len(hosts))
	sem := make(chan struct{}, 2) // 信号量：最多并发 2 台
	var wg sync.WaitGroup

	start := time.Now()

	for _, h := range hosts {
		cleanHost := strings.TrimSpace(h) // 修复了 steings
		if cleanHost == "" {
			continue
		}

		wg.Add(1)
		go func(host string) {
			defer wg.Done()

			sem <- struct{}{}        // 占坑
			defer func() { <-sem }() // 释放坑

			// 给这个任务设置 3 秒超时
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			// 🔥 这里要把 cmd 传进去！
			execOnHost(ctx, host, "kewenze", cmd, results)
		}(cleanHost)
	}

	wg.Wait()
	close(results)

	slog.Info("所有机器执行完毕", "duration", time.Since(start).String())

	successCount := 0
	failCount := 0
	var exportResults []ExportResult

	for res := range results {
		item := ExportResult{
			Host:     res.Host,
			Duration: res.Duration.String(),
		}

		if res.Err != nil {
			failCount++
			item.Success = false
			item.ErrorMsg = res.Err.Error()
			fmt.Printf("❌ [%s] 失败 (耗时 %v): %v\n", res.Host, res.Duration, res.Err)
		} else {
			successCount++
			item.Success = true
			item.Output = res.Output
			fmt.Printf("✅ [%s] 成功 (耗时 %v):\n%s\n", res.Host, res.Duration, res.Output)
		}

		exportResults = append(exportResults, item)
	}

	fmt.Printf("📊 统计: 共 %d 台 | ✅ 成功 %d 台 | ❌ 失败 %d 台\n", len(hosts), successCount, failCount)

	jsonData, err := json.MarshalIndent(exportResults, "", "  ")
	if err != nil {
		slog.Error("JSON 序列化失败", "error", err)
		return
	}

	err = os.WriteFile("report.json", jsonData, 0644)
	if err != nil {
		slog.Error("写入 report.json 失败", "error", err)
		return
	}

	slog.Info("巡检报告已生成", "file", "report.json")
}
