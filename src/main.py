import subprocess

awk_command = r'''LC_NUMERIC=C awk -F';' '
function ceil(x) { return (x == int(x)) ? x : int(x) + (x > 0) }
function round_up(val) { return ceil(val * 10) / 10 }
NF==2 {
    city = $1
    value = $2 + 0
    if (city in min) {
        if (value < min[city]) min[city] = value
        if (value > max[city]) max[city] = value
        sum[city] += value
        count[city]++
    } else {
        min[city] = max[city] = sum[city] = value
        count[city] = 1
    }
}
END {
    for (city in sum) {
        avg = sum[city] / count[city]
        printf "%s=%.1f/%.1f/%.1f\n", city, round_up(min[city]), round_up(avg), round_up(max[city])
    }
}' testcase.txt | sort -T /tmp --parallel=$(nproc) -t'=' -k1,1 > output.txt
'''

try:
    subprocess.run(awk_command, shell=True, check=True)
except subprocess.CalledProcessError as e:
    print(f"Error executing AWK command: {e}")
