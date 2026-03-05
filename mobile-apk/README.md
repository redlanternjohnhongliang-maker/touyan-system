# Touyan Android APK 壳工程

这是给 `https://redlantern.streamlit.app/` 的安卓壳 App（WebView）。

## 1) 打开工程
- 用 Android Studio 打开本目录：`mobile-apk/`
- 等待 Gradle 同步完成

## 2) 构建调试 APK
- 菜单：`Build > Build Bundle(s) / APK(s) > Build APK(s)`
- 生成路径：`mobile-apk/app/build/outputs/apk/debug/app-debug.apk`

## 2.1) 不装 Android Studio，直接生成可安装 APK（推荐）
- 本仓库已内置 GitHub Actions 自动打包：`.github/workflows/build-android-apk.yml`
- 触发方式：
	- 方式A：你 push 到 `main`（修改了 `mobile-apk/**` 会自动触发）
	- 方式B：GitHub 页面 `Actions > Build Android APK > Run workflow`
- 打包完成后下载：
	- 进入该次流水线页面
	- 在 `Artifacts` 下载 `touyan-debug-apk`
	- 解压后得到 `app-debug.apk`，直接发到手机安装

## 3) 改成你自己的网址
修改文件：
- `app/src/main/res/values/strings.xml`
- 将 `base_url` 替换为你的线上地址

## 4) 构建发布 APK（可安装给他人）
- 菜单：`Build > Generate Signed Bundle / APK`
- 选择 `APK`
- 创建或选择 keystore 后导出 `release apk`

## 5) 注意事项
- 这是网页壳，不是离线原生逻辑
- 若线上服务报 `source IP address not allowed`，App 内也会看到同样问题（因为请求仍走线上服务）
