#include <stdio.h>
#include <stddef.h>
#include <stdint.h>

/* Minimal reproduction of Aeron structs for sizeof/offsetof verification */

#pragma pack(push)
#pragma pack(4)
typedef struct {
    volatile int32_t frame_length;
    int8_t version;
    uint8_t flags;
    int16_t type;
} aeron_frame_header_t;
#pragma pack(pop)

typedef struct {
    aeron_frame_header_t frame_header;
    int32_t term_offset;
    int32_t session_id;
    int32_t stream_id;
    int32_t term_id;
    int64_t reserved_value;
} aeron_data_header_t;

typedef struct {
    aeron_data_header_t *frame;
    int32_t initial_term_id;
    size_t position_bits_to_shift;
    int32_t fragmented_frame_length;
    void *context;
} aeron_header_t;

typedef struct {
    uint8_t *frame_header;
    uint8_t *data;
    size_t length;
} aeron_buffer_claim_t;

#pragma pack(push)
#pragma pack(4)
typedef struct {
    int32_t frame_length;
    int8_t version;
    uint8_t flags;
    int16_t type;
    int32_t term_offset;
    int32_t session_id;
    int32_t stream_id;
    int32_t term_id;
    int64_t reserved_value;
} aeron_header_values_frame_t;
#pragma pack(pop)

#define PRINT_STRUCT(type) \
    printf("=== %s ===\n", #type); \
    printf("  sizeof = %zu\n", sizeof(type));

#define PRINT_FIELD(type, field) \
    printf("  offsetof(%s) = %zu, sizeof = %zu\n", #field, offsetof(type, field), sizeof(((type*)0)->field));

int main() {
    PRINT_STRUCT(aeron_frame_header_t);
    PRINT_FIELD(aeron_frame_header_t, frame_length);
    PRINT_FIELD(aeron_frame_header_t, version);
    PRINT_FIELD(aeron_frame_header_t, flags);
    PRINT_FIELD(aeron_frame_header_t, type);
    printf("\n");

    PRINT_STRUCT(aeron_data_header_t);
    PRINT_FIELD(aeron_data_header_t, frame_header);
    PRINT_FIELD(aeron_data_header_t, term_offset);
    PRINT_FIELD(aeron_data_header_t, session_id);
    PRINT_FIELD(aeron_data_header_t, stream_id);
    PRINT_FIELD(aeron_data_header_t, term_id);
    PRINT_FIELD(aeron_data_header_t, reserved_value);
    printf("\n");

    PRINT_STRUCT(aeron_header_t);
    PRINT_FIELD(aeron_header_t, frame);
    PRINT_FIELD(aeron_header_t, initial_term_id);
    PRINT_FIELD(aeron_header_t, position_bits_to_shift);
    PRINT_FIELD(aeron_header_t, fragmented_frame_length);
    PRINT_FIELD(aeron_header_t, context);
    printf("\n");

    PRINT_STRUCT(aeron_buffer_claim_t);
    PRINT_FIELD(aeron_buffer_claim_t, frame_header);
    PRINT_FIELD(aeron_buffer_claim_t, data);
    PRINT_FIELD(aeron_buffer_claim_t, length);
    printf("\n");

    PRINT_STRUCT(aeron_header_values_frame_t);
    PRINT_FIELD(aeron_header_values_frame_t, frame_length);
    PRINT_FIELD(aeron_header_values_frame_t, version);
    PRINT_FIELD(aeron_header_values_frame_t, flags);
    PRINT_FIELD(aeron_header_values_frame_t, type);
    PRINT_FIELD(aeron_header_values_frame_t, term_offset);
    PRINT_FIELD(aeron_header_values_frame_t, session_id);
    PRINT_FIELD(aeron_header_values_frame_t, stream_id);
    PRINT_FIELD(aeron_header_values_frame_t, term_id);
    PRINT_FIELD(aeron_header_values_frame_t, reserved_value);

    return 0;
}
