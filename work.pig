REGISTER myudfs.jar
test = load [sequence file path] using org.apache.pig.piggybank.storage.SequenceFileLoader();
test = foreach test generate FLATTEN(STRSPLIT((chararray)$1, '\t')) AS (vd:chararray, vt:chararray, ip:chararray, beacon:chararray, device:chararray);
test = foreach test generate vd as view_date, myudfs.ContentIDRetriever(beacon) as contentid;
test = filter test by contentid!=-1;
result = foreach (group test by (view_date, contentid))
generate
  FLATTEN(group) AS (view_date, contentid),
  COUNT(test) as view_count;
result = foreach result generate (long)contentid as contentid, view_date, view_count, view_date as vd;
store result into 'cuero_view_count_1' using org.apache.hive.hcatalog.pig.HCatStorer();
-- store result into 'test.out' using PigStorage('\t', '-schema');
