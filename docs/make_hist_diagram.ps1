# Generate docs/hist_layout.png: kv_hist_t (hist.h) HdrHistogram storage diagram.
Add-Type -AssemblyName System.Drawing

$W = 1440; $H = 1150
$bmp = New-Object System.Drawing.Bitmap($W, $H)
$g = [System.Drawing.Graphics]::FromImage($bmp)
$g.SmoothingMode = [System.Drawing.Drawing2D.SmoothingMode]::AntiAlias
$g.TextRenderingHint = [System.Drawing.Text.TextRenderingHint]::AntiAliasGridFit
$g.Clear([System.Drawing.Color]::FromArgb(255, 255, 255, 255))

function New-Font([string]$family, [float]$size, [System.Drawing.FontStyle]$style) {
    try { return New-Object System.Drawing.Font($family, $size, $style) }
    catch { return New-Object System.Drawing.Font("Arial", $size, $style) }
}
$fTitle = New-Font "Microsoft YaHei" 28 Bold
$fSub   = New-Font "Microsoft YaHei" 15 Regular
$fSec   = New-Font "Microsoft YaHei" 17 Bold
$fBody  = New-Font "Microsoft YaHei" 13 Regular
$fBodyB = New-Font "Microsoft YaHei" 13 Bold
$fMono  = New-Font "Consolas" 13 Regular
$fMonoB = New-Font "Consolas" 13 Bold
$fBox   = New-Font "Microsoft YaHei" 11 Regular
$fBoxB  = New-Font "Microsoft YaHei" 11 Bold

$cDark  = [System.Drawing.Color]::FromArgb(255, 40, 44, 52)
$cBlue  = [System.Drawing.Color]::FromArgb(255, 0, 112, 200)
$cRed   = [System.Drawing.Color]::FromArgb(255, 208, 40, 40)
$cGray  = [System.Drawing.Color]::FromArgb(255, 130, 130, 130)
$cGreen = [System.Drawing.Color]::FromArgb(255, 30, 140, 70)
$cFillB = [System.Drawing.Color]::FromArgb(255, 236, 246, 255)
$cFillR = [System.Drawing.Color]::FromArgb(255, 255, 238, 238)
$cFillG = [System.Drawing.Color]::FromArgb(255, 238, 250, 240)
$cFillY = [System.Drawing.Color]::FromArgb(255, 255, 250, 228)
$cBorder= [System.Drawing.Color]::FromArgb(255, 150, 165, 180)

$brDark = New-Object System.Drawing.SolidBrush($cDark)
$brBlue = New-Object System.Drawing.SolidBrush($cBlue)
$brRed  = New-Object System.Drawing.SolidBrush($cRed)
$brGray = New-Object System.Drawing.SolidBrush($cGray)
$brGreen= New-Object System.Drawing.SolidBrush($cGreen)
$brWhite= New-Object System.Drawing.SolidBrush([System.Drawing.Color]::White)
$brFillB= New-Object System.Drawing.SolidBrush($cFillB)
$brFillR= New-Object System.Drawing.SolidBrush($cFillR)
$brFillG= New-Object System.Drawing.SolidBrush($cFillG)
$brFillY= New-Object System.Drawing.SolidBrush($cFillY)
$penB   = New-Object System.Drawing.Pen($cBorder, 1.5)
$penRed = New-Object System.Drawing.Pen($cRed, 2.0)
$penBlue= New-Object System.Drawing.Pen($cBlue, 1.5)
$penGray= New-Object System.Drawing.Pen($cGray, 1.2)

function T([float]$x, [float]$y, [string]$s, $font, $brush) {
    $g.DrawString($s, $font, $brush, $x, $y)
}

function Box([float]$x, [float]$y, [float]$w, [float]$h, $fill, $pen) {
    $g.FillRectangle($fill, $x, $y, $w, $h)
    $g.DrawRectangle($pen, $x, $y, $w, $h)
}

# ---------------- Title ----------------
T 40 26 "hist.h 时延统计存储：HdrHistogram 分桶计数（kv_hist_t）" $fTitle $brDark
T 42 78 "3 位有效数字精度 · 固定内存 ~216 KiB · 内存与样本数无关 · 插入 O(1) · 无第三方依赖" $fSub $brGray

$y = 130
# ---------------- Section 1 ----------------
T 40 $y "① 存什么：counts[] 计数器数组（每格 = 一个时延桶的样本计数）" $fSec $brBlue
$y += 42
# array cells
$cellW = 96; $cellH = 52; $gap = 12
$cellY = $y
$xs = @()
# idx 0..3
for ($i = 0; $i -lt 4; $i++) {
    $x = 60 + $i * ($cellW + $gap)
    Box $x $cellY $cellW $cellH $brFillB $penBlue
    T ($x + 6) ($cellY + 16) ("[" + $i + "]") $fBoxB $brBlue
    T ($x + 6) ($cellY + 34) "= 0" $fBox $brGray
    $xs += $x
}
$ex = $xs[-1] + $cellW + $gap
T $ex ($cellY + 12) "⋯" $fMonoB $brGray
$redX = $ex + 40
Box $redX $cellY $cellW $cellH $brFillR $penRed
T ($redX + 6) ($cellY + 16) "[10871]" $fBoxB $brRed
T ($redX + 6) ($cellY + 34) "= 3" $fBox $brGray
$ex2 = $redX + $cellW + $gap
T $ex2 ($cellY + 12) "⋯" $fMonoB $brGray
$lastX = $ex2 + 40
Box $lastX $cellY $cellW $cellH $brFillB $penBlue
T ($lastX + 6) ($cellY + 16) "[27647]" $fBoxB $brBlue
T ($lastX + 6) ($cellY + 34) "= 1" $fBox $brGray
$y += $cellH + 14
T 60 $y "counts_array_length = (bucket_count + 1) × sub_bucket_half_count = 27 × 1024 = 27648" $fMono $brDark
T 60 ($y + 24) "内存 = 27648 × 8 B ≈ 216 KiB（一次 calloc，之后只做原子自增，永不扩容）" $fBody $brGray

$y += 66
# ---------------- Section 2 ----------------
T 40 $y "② 值轴分桶：低时延线性细桶 → 高时延指数粗桶（前密后疏，任意量级保持 ~3 位有效数字）" $fSec $brBlue
$y += 40
# bucket boxes: each 330 wide, 96 tall
$buckets = @(
    @{ b = 0;  r = "0 ~ 1023 ns";     w = "1 ns/桶" },
    @{ b = 1;  r = "1024 ~ 2047 ns";  w = "2 ns/桶" },
    @{ b = 2;  r = "2048 ~ 4095 ns";  w = "4 ns/桶" },
    @{ b = 3;  r = "4096 ~ 8191 ns";  w = "8 ns/桶" }
)
$bx = 60
foreach ($bk in $buckets) {
    Box $bx $y 330 96 $brFillB $penBlue
    T ($bx + 12) ($y + 12) ("bucket " + $bk.b) $fBoxB $brBlue
    T ($bx + 12) ($y + 38) $bk.r $fBody $brDark
    T ($bx + 12) ($y + 62) ("1024 个子桶 × " + $bk.w) $fBox $brGray
    $bx += 342
}
Box $bx $y 60 96 $brFillB $penGray
T ($bx + 20) ($y + 34) "⋯" $fMonoB $brGray
$bx += 72
T $bx ($y + 16) "bucket 25:" $fBoxB $brDark
T $bx ($y + 38) "34.4G ~ 68.7G ns" $fBody $brDark
T $bx ($y + 62) "（覆盖 60 s 上限）" $fBox $brGray
$y += 112
T 60 $y "规律：每进 1 级，覆盖区间 ×2、子桶宽 ×2 → 桶宽/桶起点 ≈ 1/512 ≈ 0.2% 相对精度，即任意时延量级上 ~3 位有效数字" $fBody $brGreen

$y += 52
# ---------------- Section 3 ----------------
T 40 $y "③ 记录一个样本 record(647000 ns)：三步位运算，O(1)" $fSec $brBlue
$y += 40
Box 60 $y 1320 132 $brFillR $penRed
$lineY = $y + 16
T 76 $lineY "① 钳位：  1 ≤ v ≤ 60e9 ns（越界拉到边界）" $fBody $brDark
T 76 ($lineY + 28) "② 找桶：  bucket = 64 - clz(v | 2047) - 10 = 10    → 该桶覆盖 [2^19, 2^20) = 524288 ~ 1048576 ns" $fMono $brDark
T 76 ($lineY + 56) "③ 找子桶：sub = v >> (bucket + unit_magnitude) = 647000 >> 10 = 631" $fMono $brDark
T 76 ($lineY + 84) "④ 计数：  counts[(10 << 10) + 631] = counts[10871] += 1（原子自增，即上图红色格子）" $fMonoB $brRed
$y += 132 + 12
T 60 $y "⑤ 顺带维护精确值：min_value / max_value 在 record 时实时更新，不丢真实最大值（p9999 以上靠它兜底）" $fBody $brGray
$y += 46

# ---------------- Section 4 ----------------
T 40 $y "④ 查询口径（读取走桶，返回桶代表值 = 桶上沿 (sub+1)<<bucket）" $fSec $brBlue
$y += 40
$rows = @(
    @{ k = "p50 / p90 / p99 / p9999"; v = "从 index 0 累加 counts，直到 ≥ 目标百分位样本数 → 返回该桶上沿（略偏高）" },
    @{ k = "mean";                     v = "Σ(桶上沿 × 该桶计数) / total_count，加权平均" },
    @{ k = "min / max";                v = "精确 min_value / max_value（record 时直接记录，非桶代表值）" },
    @{ k = "merge / 采样窗口";          v = "桶逐项相加；min 取小、max 取大（kv_hist_merge / merge_snapshot / delta）" }
)
$rowY = $y
foreach ($row in $rows) {
    Box 60 $rowY 280 40 $brFillY $penGray
    T 72 ($rowY + 11) $row.k $fBodyB $brDark
    Box 340 $rowY 1040 40 $brFillG $penGray
    T 352 ($rowY + 11) $row.v $fBody $brDark
    $rowY += 48
}
$y = $rowY + 8
T 60 $y "推论：采样窗口的 pmax 是桶上沿（≥ 真实值），总汇总 pmax 是精确 max_value → 窗口 pmax 略大于汇总 pmax（如 2842 vs 2839 us）" $fBody $brGray

$bmp.Save((Join-Path $PSScriptRoot "hist_layout.png"), [System.Drawing.Imaging.ImageFormat]::Png)
$g.Dispose(); $bmp.Dispose()
Write-Output "saved: $(Join-Path $PSScriptRoot 'hist_layout.png')"
