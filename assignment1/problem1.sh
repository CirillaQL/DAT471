#!/bin/bash

set -euo pipefail

# Collect CPU information and deserialize "key: value" lines into a map.
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

get_lscpu_value() {
  local field_name

  for field_name in "$@"; do
    if [[ -n "${lscpu_fields[$field_name]:-}" ]]; then
      printf '%s\n' "${lscpu_fields[$field_name]}"
      return 0
    fi
  done

  printf '%s\n' "Unknown"
}

get_cpu_freq_from_sysfs() {
  local sysfs_field=$1
  local file
  local min_value=""
  local max_value=""
  local raw_value
  local mhz_value

  for file in /sys/devices/system/cpu/cpu[0-9]*/cpufreq/"$sysfs_field"; do
    [[ -f "$file" ]] || continue
    raw_value=$(<"$file")
    [[ "$raw_value" =~ ^[0-9]+$ ]] || continue
    mhz_value=$(awk "BEGIN {printf \"%.3f\", $raw_value / 1000}")

    if [[ -z "$min_value" ]] || awk "BEGIN {exit !($mhz_value < $min_value)}"; then
      min_value=$mhz_value
    fi

    if [[ -z "$max_value" ]] || awk "BEGIN {exit !($mhz_value > $max_value)}"; then
      max_value=$mhz_value
    fi
  done

  if [[ -n "$min_value" && -n "$max_value" ]]; then
    printf '%s\n' "$min_value $max_value"
  else
    printf '%s\n' "Unknown Unknown"
  fi
}

model=${lscpu_fields["Model name"]:-Unknown}
nominal_clock_mhz=$(get_lscpu_value "CPU max MHz" "Max MHz")
current_clock_mhz=$(get_lscpu_value "CPU MHz" "Current MHz")
min_clock_mhz=$(get_lscpu_value "CPU min MHz" "Min MHz")
max_clock_mhz=$(get_lscpu_value "CPU max MHz" "Max MHz")
sockets=$(get_lscpu_value "Socket(s)")
cores_per_socket=$(get_lscpu_value "Core(s) per socket")
hardware_threads=$(get_lscpu_value "CPU(s)")
total_cores=$((sockets * cores_per_socket))
architecture=${lscpu_fields["Architecture"]:-Unknown}
cache_line_length=$(getconf LEVEL1_DCACHE_LINESIZE)
l1d_cache=$(get_lscpu_value "L1d" "L1d cache")
l1i_cache=$(get_lscpu_value "L1i" "L1i cache")
l2_cache=$(get_lscpu_value "L2" "L2 cache")
l3_cache=$(get_lscpu_value "L3" "L3 cache")
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

if [[ "$nominal_clock_mhz" != "Unknown" ]]; then
  clock_frequency="$nominal_clock_mhz MHz"
elif [[ "$min_clock_mhz" != "Unknown" && "$max_clock_mhz" != "Unknown" ]]; then
  clock_frequency="min ${min_clock_mhz} MHz, max ${max_clock_mhz} MHz"
elif [[ "$current_clock_mhz" != "Unknown" ]]; then
  clock_frequency="$current_clock_mhz MHz"
else
  read -r sysfs_min_clock_mhz sysfs_max_clock_mhz <<< "$(get_cpu_freq_from_sysfs scaling_cur_freq)"

  if [[ "$sysfs_min_clock_mhz" != "Unknown" && "$sysfs_max_clock_mhz" != "Unknown" ]]; then
    clock_frequency="min ${sysfs_min_clock_mhz} MHz, max ${sysfs_max_clock_mhz} MHz"
  else
    proc_cpu_mhz_range=$(awk '
      /^cpu MHz[[:space:]]*:/ {
        value = $4
        if (count == 0 || value < min) min = value
        if (count == 0 || value > max) max = value
        count++
      }
      END {
        if (count > 0) printf "%.3f %.3f\n", min, max
        else print "Unknown Unknown"
      }
    ' /proc/cpuinfo)

    read -r proc_min_clock_mhz proc_max_clock_mhz <<< "$proc_cpu_mhz_range"

    if [[ "$proc_min_clock_mhz" != "Unknown" && "$proc_max_clock_mhz" != "Unknown" ]]; then
      clock_frequency="min ${proc_min_clock_mhz} MHz, max ${proc_max_clock_mhz} MHz"
    else
      clock_frequency="Unknown"
    fi
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
