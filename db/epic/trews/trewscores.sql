select top 1000 cnt.fsd_id, cnt.recorded_time,
  cnt.meas_value as "count",
  score.meas_value as score,
  threshold.meas_value as threshold,
  flag.meas_value as flag,
  trews_version.meas_value as "version"
from CLARITY.dbo.IP_FLWSHT_MEAS cnt
left join CLARITY.dbo.IP_FLWSHT_MEAS score on score.recorded_time = cnt.recorded_time and score.fsd_id = cnt.fsd_id
left join CLARITY.dbo.IP_FLWSHT_MEAS threshold on threshold.recorded_time = cnt.recorded_time and threshold.fsd_id = cnt.fsd_id
left join CLARITY.dbo.IP_FLWSHT_MEAS flag on flag.recorded_time = cnt.recorded_time and flag.fsd_id = cnt.fsd_id
left join CLARITY.dbo.IP_FLWSHT_MEAS trews_version on trews_version.recorded_time = cnt.recorded_time and trews_version.fsd_id = cnt.fsd_id
where cnt.FLO_meas_id = '9490' and score.flo_meas_id = '9485' and threshold.flo_meas_id = '94851' and flag.flo_meas_id = '94852' and trews_version.flo_meas_id = '94853'
and cnt.recorded_time > '2017-11-15' and cnt.meas_value = 4 and score.meas_value <= threshold.meas_value
order by cnt.fsd_id, recorded_time
;
