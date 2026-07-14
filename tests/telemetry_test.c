#include "../telemetry.h"
#include "../stats.h"

#include <stdio.h>
#include <string.h>

static int failures;
#define CHECK(expr, name) do { if (!(expr)) { \
    fprintf(stderr, "FAIL: %s\n", name); failures++; } } while (0)

int main(void)
{
    transfer_class_summary_t classes[TRANSFER_CLASS_COUNT];
    transfer_rate_summary_t rate;
    char formatted[32];

    telemetry_init();
    telemetry_get(classes, &rate);
    CHECK(rate.samples == 0 && classes[TRANSFER_SMALL].count == 0,
          "empty populations");

    telemetry_note_file(TRANSFER_SMALL, 0, 0, 1000);
    telemetry_note_file(TRANSFER_SMALL, 1024, 1024, 1000000000);
    telemetry_note_file(TRANSFER_LARGE, 1024 * 1024, 1024 * 1024, 1000000);
    telemetry_note_rate_window(0, 1000000000);
    telemetry_note_rate_window(1024, 1000000000);
    telemetry_note_rate_window(1024 * 1024, 1000000000);
    telemetry_get(classes, &rate);

    CHECK(classes[TRANSFER_SMALL].count == 2,
          "zero-byte file included in latency population");
    CHECK(classes[TRANSFER_SMALL].rate_samples == 1,
          "zero-byte file excluded from throughput population");
    CHECK(classes[TRANSFER_SMALL].latency_p50_ns <=
              classes[TRANSFER_SMALL].latency_p99_ns,
          "latency percentiles ordered");
    CHECK(rate.samples == 3 && rate.min_bps == 0 &&
              rate.p10_bps <= rate.p50_bps &&
              rate.p50_bps <= rate.p90_bps &&
              rate.p90_bps <= rate.p99_bps &&
              rate.max_bps == 1024 * 1024,
          "rolling rate exact bounds and ordered percentiles");

    stats_format_rate(1024, 1.0, formatted, sizeof(formatted));
    CHECK(strcmp(formatted, "1.00 KiB/s") == 0, "adaptive KiB rate");
    stats_format_rate(0, 0.0, formatted, sizeof(formatted));
    CHECK(strcmp(formatted, "0.00 B/s") == 0, "adaptive empty rate");

    if (failures) return 1;
    printf("telemetry_test: all checks passed\n");
    return 0;
}
