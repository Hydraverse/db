#!/bin/sh

pgxn load --yes -U "$POSTGRES_USER" -d "$POSTGRES_DB" quantile

echo "alter database ${POSTGRES_DB} set timezone to 'Etc/UTC';" | \
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" "$POSTGRES_DB"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" "$POSTGRES_DB" <<EOF
CREATE SCHEMA stat;
EOF

#psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" "$POSTGRES_DB" <<EOF
#CREATE VIEW stat.quant_net_weight(count, median_1h, median_1d, median_1w, median_1m) as
#    SELECT (SELECT count(pkid) FROM stat.stat)                               AS count,
#           (SELECT quantile(stat.net_weight, 0.5::double precision) AS quantile
#            FROM stat.stat
#            WHERE stat."time" > (now()::timestamp without time zone - '01:00:00'::interval)) AS median_1h,
#           (SELECT quantile(stat.net_weight, 0.5::double precision) AS quantile
#            FROM stat.stat
#            WHERE stat."time" > (now()::timestamp without time zone - '1 day'::interval))    AS median_1d,
#           (SELECT quantile(stat.net_weight, 0.5::double precision) AS quantile
#            FROM stat.stat
#            WHERE stat."time" > (now()::timestamp without time zone - '7 days'::interval))   AS median_1w,
#           (SELECT quantile(stat.net_weight, 0.5::double precision) AS quantile
#            FROM stat.stat
#            WHERE stat."time" > (now()::timestamp without time zone - '1 mon'::interval))    AS median_1m;
#EOF
