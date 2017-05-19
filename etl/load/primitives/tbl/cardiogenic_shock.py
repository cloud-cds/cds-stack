import asyncio
import etl.load.primitives.tbl.clean_tbl as clean_tbl
from etl.load.primitives.tbl.derive_helper import *


async def calculate_major_blood_loss(output_fid, input_fid_string, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental):
  """
  fid_input should be name of the feature for which change is to be computed
  fid should be <fid of old feather>_change
  """


  """
  
  write helpers to clean the right tables. Right helpers to return the temp table name of all of the tables
  """


  log.info(
    """
             .<@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@>. 
            .<@@@@@@   $$$$$$$$$$$$$$$$$$$$$\^^^^^^/$$$$@@@>. 
         .<@@@@@<   .$$$$$'~       '~'$$$$$$$\  /$$$$$$>@@@@@>. 
      .<@@@@@<'   o$$$$$$                `'$$$$$$$$$$$$  '>@@@@@>. 
   .<@@@@@<'    o$$$$$$oo.                  )$$$$$$$$$$     '>@@@@@>. 
   '<@@@@@<    o$$$$$$$$$$$.                                 >@@@@@>' 
     '<@@@@<  o$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$oooooo...    >@@@@>' 
       '@@@@< $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$)>@@@@>' 
         '<@@@@$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$@@@@>' 
           '<@@@@$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$@@@@>' 
             '<@@@@<    .oooo.                    .$$@@@@>' 
               '<@@@@oo$$$$$$$o..             ..o$$@@@@>' 
                 '<@@@@$$$$$$$$$$$$$oooooooo$$$$$@@@@>' 
                   '<@@@@'$$$$$$$$$$$$$$$$$$$$$@@@@>' 
                     '<@@@@<   ~"SSSSSS"~   >@@@@>' 
                       '<@@@@<            >@@@@>' 
                         '<@@@@<        >@@@@>' 
                           '<@@@@<    >@@@@>' 
                             '<@@@@<>@@@@>' 
                               '<@@@@@@>' 
                                 '<@@>' 
                                   ^^          
  """)

  # Make sure the fid is correct (fid_input can be anything)
  assert output_fid == 'major_blood_loss', 'wrong fid %s' % output_fid
  input_fid_list = [item.strip() for item in input_fid_string.split(',')]

  destination_tbl = derive_feature_addr[output_fid]['twf_table_temp']



  source_cdm_twf_tbl = derive_feature_addr[input_fid_string]['twf_table_temp'] if input_fid_string in derive_feature_addr else get_src_twf_table(derive_feature_addr)
  await conn.execute(clean_tbl.cdm_t_clean(output_fid, dataset_id=dataset_id, incremental=incremental))

  sql = """select * from cdm_twf where dataset_id = 1"""


  log.info("time_since_last_measured:%s" % sql)
  return output_fid