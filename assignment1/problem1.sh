#!/bin/bash

set -euo pipefail

cpu_info=$(lscpu)
declare -A lscpu_fields

while IFS= read -r line; do
  [[ "$line" == *:* ]] || continue

  key=${line%%:*}
  value=${line#*:}
  key=$(printf '%s' "$key" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')
  value=$(printf '%s' "$value" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')

  [[ -n "$key" ]] || continue
  lscpu_fields["$key"]="$value"
done <<< "$cpu_info"

model=${lscpu_fields["Model name"]:-Unknown}
sockets=${lscpu_fields["Socket(s)"]:-0}
cores_per_socket=${lscpu_fields["Core(s) per socket"]:-0}
hardware_threads=${lscpu_fields["CPU(s)"]:-0}
total_cores=$((sockets * cores_per_socket))
architecture=${lscpu_fields["Architecture"]:-Unknown}
cache_line_length=$(getconf LEVEL1_DCACHE_LINESIZE)
l1d_cache=${lscpu_fields["L1d cache"]:-${lscpu_fields["L1d"]:-Unknown}}
l1i_cache=${lscpu_fields["L1i cache"]:-${lscpu_fields["L1i"]:-Unknown}}
l2_cache=${lscpu_fields["L2 cache"]:-${lscpu_fields["L2"]:-Unknown}}
l3_cache=${lscpu_fields["L3 cache"]:-${lscpu_fields["L3"]:-Unknown}}
gpu_count=0
gpu_models="Unknown"
gpu_memory="Unknown"

if command -v nvidia-smi >/dev/null 2>&1; then
  gpu_info=$(nvidia-smi --query-gpu=name,memory.total --format=csv,noheader 2>/dev/null || true)

  if [[ -n "$gpu_info" ]]; then
    gpu_count=0
    gpu_models=""
    gpu_memory=""

    while IFS= read -r gpu_line; do
      [[ -n "$gpu_line" ]] || continue
      gpu_count=$((gpu_count + 1))

      gpu_model=${gpu_line%%,*}
      gpu_ram=${gpu_line#*,}

      gpu_model=$(printf '%s' "$gpu_model" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')
      gpu_ram=$(printf '%s' "$gpu_ram" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')

      if [[ -z "$gpu_models" ]]; then
        gpu_models="$gpu_model"
        gpu_memory="$gpu_ram"
      else
        gpu_models="$gpu_models; $gpu_model"
        gpu_memory="$gpu_memory; $gpu_ram"
      fi
    done <<< "$gpu_info"
  fi
fi

if [[ -n "${lscpu_fields["CPU max MHz"]:-}" ]]; then
  clock_frequency="${lscpu_fields["CPU max MHz"]} MHz"
elif [[ -n "${lscpu_fields["Max MHz"]:-}" ]]; then
  clock_frequency="${lscpu_fields["Max MHz"]} MHz"
else
  min_clock=$(awk 'BEGIN {min=""} {if (min=="" || $1 < min) min=$1} END {print min}' /sys/devices/system/cpu/cpu*/cpufreq/scaling_cur_freq 2>/dev/null)
  max_clock=$(awk 'BEGIN {max=0} {if ($1 > max) max=$1} END {print max}' /sys/devices/system/cpu/cpu*/cpufreq/scaling_cur_freq 2>/dev/null)

  if [[ -n "$min_clock" && -n "$max_clock" && "$min_clock" != "0" && "$max_clock" != "0" ]]; then
    clock_frequency="min $(awk "BEGIN {printf \"%.3f\", $min_clock / 1000}") MHz, max $(awk "BEGIN {printf \"%.3f\", $max_clock / 1000}") MHz"
  else
    clock_frequency="Unknown"
  fi
fi


echo "The model of and the clock frequency1 of the CPU: $model, $clock_frequency"
echo "The number of physical CPUs (sockets in use): $sockets"
echo "The number of cores: $total_cores"
echo "The number of hardware threads: $hardware_threads"
echo "The instruction set architecture of the CPU: $architecture"
echo "The cache line length: $cache_line_length"
echo "The amount of L1, L2, and L3 cache: L1d: $l1d_cache, L1i: $l1i_cache, L2: $l2_cache, L3: $l3_cache"
echo "The amount of system RAM: $(free -h | awk '/^Mem:/ {print $2}')"
echo "The number of GPUs and model of the GPU(s): $gpu_count GPU(s), Model(s): $gpu_models"
echo "The amount of RAM on the GPU(s): $gpu_memory"
echo "The type of filesystem of /data: $(df -T /data | tail -1 | awk '{print $2}')"
echo "The total amount of disk space and the amount of free space on /data: $(df -h /data | tail -1 | awk '{print $2 " total, " $4 " free"}')"
echo "The filename and the version of the default Python 3 interpreter available on the system (globally installed): $(python3 --version 2>/dev/null || echo "Not found")"
