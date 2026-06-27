#!/bin/bash

# --- Step 1: Define Core Paths ---
# 请确保这些路径是正确的
export OPENGAUSS_CODE_PATH="/home/zwx/openGauss-server"
export OPENGAUSS_INSTALL_PATH="${OPENGAUSS_CODE_PATH}/mppdb_temp_install"
export BINARYLIBS_PATH="/home/zwx/binarylibs"
export DATA_DIR="${OPENGAUSS_INSTALL_PATH}/data/single_node"

echo "--- Using Installation Path: ${OPENGAUSS_INSTALL_PATH}"
echo "--- Using Binarylibs Path: ${BINARYLIBS_PATH}"
echo "--- Using Data Path: ${DATA_DIR}"

# --- Step 2: Create a Clean and Correct Environment ---
# 清理可能存在的旧环境变量
# (这是一个可选的防御性步骤，但有助于避免问题)
unset LD_LIBRARY_PATH

# 关键：将我们新安装的lib目录放在最前面！
export LD_LIBRARY_PATH=${OPENGAUSS_INSTALL_PATH}/lib

# 添加其他必要的库路径 (从binarylibs中)
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${BINARYLIBS_PATH}/dependency/openssl/lib
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${BINARYLIBS_PATH}/dependency/onnxruntime/lib
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${BINARYLIBS_PATH}/dependency/protobuf/lib

echo "--- LD_LIBRARY_PATH is set to: ${LD_LIBRARY_PATH}"

# --- Step 3: Clean Up Previous Failed Installation ---
echo "--- Cleaning up previous installation attempt..."
# 强制杀死任何可能在运行的旧进程
killall -9 gaussdb > /dev/null 2>&1
# 删除可能残留的数据目录
rm -rf "${DATA_DIR}"
echo "--- Cleanup complete."

# --- Step 4: Run the Installation ---
echo "--- Starting install.sh..."
cd "${OPENGAUSS_INSTALL_PATH}/simpleInstall/"

# 使用设置好的干净环境来执行安装脚本
sh install.sh -w "YourPassword@123" -p 16000

echo "--- Installation script finished."