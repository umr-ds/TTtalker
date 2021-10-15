# Packet Formats

## Data Packet (rev 3.1)

| receiver address | sender address | packet type | packet_number | timestamp | t_ref_0 | t_heat_0 | growth   | battery  | # bits | air_humid | air_temp | gravity * 6              | t_ref_1 | t_heat_1 | moist |
|:-----------------|:---------------|:------------|:--------------|:----------|:--------|:---------|:---------|:---------|:-------|:----------|:---------|:-------------------------|:--------|:---------|:------|
| I                | I              | B           | B             | I         | h       | h        | I        | I        | B      | B         | h        | hhhhhh                   | h       | h        | h     |
| 180103c2         | 63079921       | 45          | 11            | c83f6961  | e500    | 38ff     | ac020000 | 8ba00000 | 11     | 37        | cb00     | 83ff8e00de0f000000000000 | e900    | 38ff     | a938  |