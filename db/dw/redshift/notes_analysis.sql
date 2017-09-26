drop table if exists infection_keywords;
create table infection_keywords(keyword varchar(1024));
insert into infection_keywords (keyword) values
('(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])'),
('[uU]rinary [tT]ract [iI]nfection'),
(' CAUTI '),
('[uU]rosepsis'),
('[pP]yelo(nephritis)*'),
('[bB]iliary [sS]epsis'),
('[cC]holecystitis'),
('[cC]ystitis'),
('[aA]ppendicitis'),
('[cC]holangitis'),
('[cC]holelithiasis'),
('[hH]epatic [aA]bscess'),
('[iI]ntra [aA]bdominal [sS]epsis'),
('[pP]neumoperitoneum'),
('[pP]neumotosis'),
('[pP]eritonitis'),
('[sS]pontaneous [bB]acerial [pP]eritonitis'),
('(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*'),
('[cC]olitis'),
('([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia'),
('[^a-zA-Z]PNA[^a-zA-Z]'),
(' HCAP '),
(' CAP '),
('[eE]mpyema'),
('[lL]ine [sS]epsis'),
('[eE]ndocarditis'),
('[bB]lood [sS]tream [iI]nfection'),
('[rRlL]*(ight)*(eft)*[ ]*[lL]*(ower)*[ ]*[eE]*(xtremity)*[ ]*[cC]ellulitis'),
('[mM]ediastinitis'),
('[cC]entral [lL]ine [iI]nfection'),
('CLABSI'),
('[mM]yocarditis'),
('[oO]steomyelitis'),
('[nN]ecrotizing [fF]asciitis'),
('[pP]seudomonas'),
('[iI]nfluenza'),
('([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess')
;

drop table if exists negation_keywords;
create table negation_keywords(keyword varchar(1024));
insert into negation_keywords (keyword) values
('[hH](istory)*(x)* of( recurrent|recent)*'),
('[hH]/[oO]'),
('[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*'),
('[nN]ot'),
('[Nn]eg(ative)*'),
('[rR]ecently had'),
('[dD]enies'),
('[dD]enies any'),
('[rR]ecently hospitalized for'),
('[rR]ecurrent'),
('[aA]bsence of'),
('[hH]ave had'),
('•[ ]*')
;


--------------------------------------------------------
-- Notes processing on cdm tables.
--------------------------------------------------------

-- Python helper function to extract json array ngrams from documents with matches.
create function doc2ngrams(doc varchar(max)) returns varchar(max) stable
as $$
  def ngram_doc(doc):
    import re
    import json
    ngrams = []
    words = re.split('\s+', doc)
    max_word_idx = len(words)
    for iw in enumerate(words):
      if re.match('\#\#\*\*(.+?)\*\*\#\#', iw[1]):
        ngrams.append(words[max(0, iw[0]-3) : min(iw[0]+4, max_word_idx)])

    # Build a json array
    return json.dumps(ngrams)

  return ngram_doc(doc)
$$ language plpythonu;


create table cdm_processed_notes_d6 as
select dataset_id, pat_id, note_id, note_type, note_status, start_ts,
       doc2ngrams(note_body1) as ngrams1,
       doc2ngrams(note_body2) as ngrams2,
       doc2ngrams(note_body3) as ngrams3
from (
select  dataset_id, pat_id, note_id, note_type, note_status, start_ts,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(note_body1, '([bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis)', '##**\\1**##')
                                 , '([uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis)', '##**\\1**##')
                                 , '([uU]rosepsis|[hH]epatic [aA]bscess)', '##**\\1**##')
                                 , '(([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia)', '##**\\1**##')
                                 , '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)', '##**\\1**##')
                                 , '([iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis)', '##**\\1**##')
                                 , '([lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*)', '##**\\1**##')
                                 , '([iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis)', '##**\\1**##')
                                 , '([pP]neumotosis| CAP |CLABSI)', '##**\\1**##')
          as note_body1,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(note_body2, '([bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis)', '##**\\1**##')
                                 , '([uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis)', '##**\\1**##')
                                 , '([uU]rosepsis|[hH]epatic [aA]bscess)', '##**\\1**##')
                                 , '(([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia)', '##**\\1**##')
                                 , '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)', '##**\\1**##')
                                 , '([iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis)', '##**\\1**##')
                                 , '([lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*)', '##**\\1**##')
                                 , '([iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis)', '##**\\1**##')
                                 , '([pP]neumotosis| CAP |CLABSI)', '##**\\1**##')
          as note_body2,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(note_body3, '([bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis)', '##**\\1**##')
                                 , '([uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis)', '##**\\1**##')
                                 , '([uU]rosepsis|[hH]epatic [aA]bscess)', '##**\\1**##')
                                 , '(([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia)', '##**\\1**##')
                                 , '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)', '##**\\1**##')
                                 , '([iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis)', '##**\\1**##')
                                 , '([lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*)', '##**\\1**##')
                                 , '([iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis)', '##**\\1**##')
                                 , '([pP]neumotosis| CAP |CLABSI)', '##**\\1**##')
          as note_body3
from (
select  N.dataset_id, N.pat_id, N.note_id, N.note_type, N.note_status,
        json_extract_path_text(N.dates, 'entry_instant_dttm')::timestamptz as start_ts,
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(N.note_body1, '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rosepsis|[hH]epatic [aA]bscess', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[pP]neumotosis| CAP |CLABSI', 'NEGATED_PHRASE')
          as note_body1,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(N.note_body2, '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rosepsis|[hH]epatic [aA]bscess', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[pP]neumotosis| CAP |CLABSI', 'NEGATED_PHRASE')
          as note_body2,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(N.note_body3, '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rosepsis|[hH]epatic [aA]bscess', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[pP]neumotosis| CAP |CLABSI', 'NEGATED_PHRASE')
          as note_body3


from (
  select N.dataset_id, N.pat_id, N.note_id, N.note_type, N.note_status, N.dates,
         substring(N.note_body1 for 63035) as note_body1,
         substring(N.note_body1 from 63036 for 2500) || substring(N.note_body2 for 60035) as note_body2,
         substring(N.note_body2 from 60036 for 5000) || N.note_body3 as note_body3
  from cdm_notes N
  where N.dataset_id = 6
  and N.author_type <> 'Pharmacist'
) N
) NEG
where (
           regexp_instr(note_body1, '[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis') > 0
        or regexp_instr(note_body1, '[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis') > 0
        or regexp_instr(note_body1, '[uU]rosepsis|[hH]epatic [aA]bscess') > 0
        or regexp_instr(note_body1, '([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia') > 0
        or regexp_instr(note_body1, '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)') > 0
        or regexp_instr(note_body1, '[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis') > 0
        or regexp_instr(note_body1, '[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*') > 0
        or regexp_instr(note_body1, '[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis') > 0
        or regexp_instr(note_body1, '[pP]neumotosis| CAP |CLABSI') > 0

        or regexp_instr(note_body2, '[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis') > 0
        or regexp_instr(note_body2, '[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis') > 0
        or regexp_instr(note_body2, '[uU]rosepsis|[hH]epatic [aA]bscess') > 0
        or regexp_instr(note_body2, '([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia') > 0
        or regexp_instr(note_body2, '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)') > 0
        or regexp_instr(note_body2, '[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis') > 0
        or regexp_instr(note_body2, '[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*') > 0
        or regexp_instr(note_body2, '[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis') > 0
        or regexp_instr(note_body2, '[pP]neumotosis| CAP |CLABSI') > 0

        or regexp_instr(note_body3, '[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis') > 0
        or regexp_instr(note_body3, '[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis') > 0
        or regexp_instr(note_body3, '[uU]rosepsis|[hH]epatic [aA]bscess') > 0
        or regexp_instr(note_body3, '([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia') > 0
        or regexp_instr(note_body3, '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)') > 0
        or regexp_instr(note_body3, '[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis') > 0
        or regexp_instr(note_body3, '[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*') > 0
        or regexp_instr(note_body3, '[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis') > 0
        or regexp_instr(note_body3, '[pP]neumotosis| CAP |CLABSI') > 0
      )
) R
;







create table cdm_processed_notes_d1 as
select dataset_id, pat_id, note_id, note_type, note_status, start_ts,
       doc2ngrams(note_body1) as ngrams1,
       doc2ngrams(note_body2) as ngrams2,
       doc2ngrams(note_body3) as ngrams3
from (
select  dataset_id, pat_id, note_id, note_type, note_status, start_ts,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(note_body1, '([bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis)', '##**\\1**##')
                                 , '([uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis)', '##**\\1**##')
                                 , '([uU]rosepsis|[hH]epatic [aA]bscess)', '##**\\1**##')
                                 , '(([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia)', '##**\\1**##')
                                 , '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)', '##**\\1**##')
                                 , '([iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis)', '##**\\1**##')
                                 , '([lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*)', '##**\\1**##')
                                 , '([iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis)', '##**\\1**##')
                                 , '([pP]neumotosis| CAP |CLABSI)', '##**\\1**##')
          as note_body1,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(note_body2, '([bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis)', '##**\\1**##')
                                 , '([uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis)', '##**\\1**##')
                                 , '([uU]rosepsis|[hH]epatic [aA]bscess)', '##**\\1**##')
                                 , '(([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia)', '##**\\1**##')
                                 , '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)', '##**\\1**##')
                                 , '([iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis)', '##**\\1**##')
                                 , '([lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*)', '##**\\1**##')
                                 , '([iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis)', '##**\\1**##')
                                 , '([pP]neumotosis| CAP |CLABSI)', '##**\\1**##')
          as note_body2,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(note_body3, '([bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis)', '##**\\1**##')
                                 , '([uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis)', '##**\\1**##')
                                 , '([uU]rosepsis|[hH]epatic [aA]bscess)', '##**\\1**##')
                                 , '(([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia)', '##**\\1**##')
                                 , '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)', '##**\\1**##')
                                 , '([iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis)', '##**\\1**##')
                                 , '([lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*)', '##**\\1**##')
                                 , '([iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis)', '##**\\1**##')
                                 , '([pP]neumotosis| CAP |CLABSI)', '##**\\1**##')
          as note_body3
from (
select  N.dataset_id, N.pat_id, N.note_id, N.note_type, N.note_status,
        json_extract_path_text(N.dates, 'entry_instant_dttm')::timestamptz as start_ts,
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(N.note_body1, '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rosepsis|[hH]epatic [aA]bscess', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[pP]neumotosis| CAP |CLABSI', 'NEGATED_PHRASE')
          as note_body1,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(N.note_body2, '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rosepsis|[hH]epatic [aA]bscess', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[pP]neumotosis| CAP |CLABSI', 'NEGATED_PHRASE')
          as note_body2,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(N.note_body3, '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rosepsis|[hH]epatic [aA]bscess', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[pP]neumotosis| CAP |CLABSI', 'NEGATED_PHRASE')
          as note_body3


from (
  select N.dataset_id, N.pat_id, N.note_id, N.note_type, N.note_status, N.dates,
         substring(N.note_body1 for 63035) as note_body1,
         substring(N.note_body1 from 63036 for 2500) || substring(N.note_body2 for 60035) as note_body2,
         substring(N.note_body2 from 60036 for 5000) || N.note_body3 as note_body3
  from cdm_notes N
  where N.dataset_id = 1
  and N.author_type <> 'Pharmacist'
) N
) NEG
where (
           regexp_instr(note_body1, '[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis') > 0
        or regexp_instr(note_body1, '[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis') > 0
        or regexp_instr(note_body1, '[uU]rosepsis|[hH]epatic [aA]bscess') > 0
        or regexp_instr(note_body1, '([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia') > 0
        or regexp_instr(note_body1, '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)') > 0
        or regexp_instr(note_body1, '[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis') > 0
        or regexp_instr(note_body1, '[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*') > 0
        or regexp_instr(note_body1, '[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis') > 0
        or regexp_instr(note_body1, '[pP]neumotosis| CAP |CLABSI') > 0

        or regexp_instr(note_body2, '[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis') > 0
        or regexp_instr(note_body2, '[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis') > 0
        or regexp_instr(note_body2, '[uU]rosepsis|[hH]epatic [aA]bscess') > 0
        or regexp_instr(note_body2, '([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia') > 0
        or regexp_instr(note_body2, '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)') > 0
        or regexp_instr(note_body2, '[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis') > 0
        or regexp_instr(note_body2, '[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*') > 0
        or regexp_instr(note_body2, '[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis') > 0
        or regexp_instr(note_body2, '[pP]neumotosis| CAP |CLABSI') > 0

        or regexp_instr(note_body3, '[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis') > 0
        or regexp_instr(note_body3, '[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis') > 0
        or regexp_instr(note_body3, '[uU]rosepsis|[hH]epatic [aA]bscess') > 0
        or regexp_instr(note_body3, '([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia') > 0
        or regexp_instr(note_body3, '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)') > 0
        or regexp_instr(note_body3, '[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis') > 0
        or regexp_instr(note_body3, '[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*') > 0
        or regexp_instr(note_body3, '[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis') > 0
        or regexp_instr(note_body3, '[pP]neumotosis| CAP |CLABSI') > 0
      )
) R
;






create table cdm_processed_notes_d10 as
select dataset_id, pat_id, note_id, note_type, note_status, start_ts,
       doc2ngrams(note_body1) as ngrams1,
       doc2ngrams(note_body2) as ngrams2,
       doc2ngrams(note_body3) as ngrams3
from (
select  dataset_id, pat_id, note_id, note_type, note_status, start_ts,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(note_body1, '([bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis)', '##**\\1**##')
                                 , '([uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis)', '##**\\1**##')
                                 , '([uU]rosepsis|[hH]epatic [aA]bscess)', '##**\\1**##')
                                 , '(([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia)', '##**\\1**##')
                                 , '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)', '##**\\1**##')
                                 , '([iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis)', '##**\\1**##')
                                 , '([lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*)', '##**\\1**##')
                                 , '([iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis)', '##**\\1**##')
                                 , '([pP]neumotosis| CAP |CLABSI)', '##**\\1**##')
          as note_body1,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(note_body2, '([bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis)', '##**\\1**##')
                                 , '([uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis)', '##**\\1**##')
                                 , '([uU]rosepsis|[hH]epatic [aA]bscess)', '##**\\1**##')
                                 , '(([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia)', '##**\\1**##')
                                 , '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)', '##**\\1**##')
                                 , '([iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis)', '##**\\1**##')
                                 , '([lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*)', '##**\\1**##')
                                 , '([iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis)', '##**\\1**##')
                                 , '([pP]neumotosis| CAP |CLABSI)', '##**\\1**##')
          as note_body2,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(note_body3, '([bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis)', '##**\\1**##')
                                 , '([uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis)', '##**\\1**##')
                                 , '([uU]rosepsis|[hH]epatic [aA]bscess)', '##**\\1**##')
                                 , '(([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia)', '##**\\1**##')
                                 , '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)', '##**\\1**##')
                                 , '([iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis)', '##**\\1**##')
                                 , '([lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*)', '##**\\1**##')
                                 , '([iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis)', '##**\\1**##')
                                 , '([pP]neumotosis| CAP |CLABSI)', '##**\\1**##')
          as note_body3
from (
select  N.dataset_id, N.pat_id, N.note_id, N.note_type, N.note_status,
        json_extract_path_text(N.dates, 'entry_instant_dttm')::timestamptz as start_ts,
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(N.note_body1, '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rosepsis|[hH]epatic [aA]bscess', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[pP]neumotosis| CAP |CLABSI', 'NEGATED_PHRASE')
          as note_body1,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(N.note_body2, '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rosepsis|[hH]epatic [aA]bscess', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[pP]neumotosis| CAP |CLABSI', 'NEGATED_PHRASE')
          as note_body2,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(N.note_body3, '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rosepsis|[hH]epatic [aA]bscess', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[pP]neumotosis| CAP |CLABSI', 'NEGATED_PHRASE')
          as note_body3


from (
  select N.dataset_id, N.pat_id, N.note_id, N.note_type, N.note_status, N.dates,
         substring(N.note_body1 for 63035) as note_body1,
         substring(N.note_body1 from 63036 for 2500) || substring(N.note_body2 for 60035) as note_body2,
         substring(N.note_body2 from 60036 for 5000) || N.note_body3 as note_body3
  from cdm_notes N
  where N.dataset_id = 10
  and N.author_type <> 'Pharmacist'
) N
) NEG
where (
           regexp_instr(note_body1, '[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis') > 0
        or regexp_instr(note_body1, '[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis') > 0
        or regexp_instr(note_body1, '[uU]rosepsis|[hH]epatic [aA]bscess') > 0
        or regexp_instr(note_body1, '([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia') > 0
        or regexp_instr(note_body1, '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)') > 0
        or regexp_instr(note_body1, '[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis') > 0
        or regexp_instr(note_body1, '[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*') > 0
        or regexp_instr(note_body1, '[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis') > 0
        or regexp_instr(note_body1, '[pP]neumotosis| CAP |CLABSI') > 0

        or regexp_instr(note_body2, '[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis') > 0
        or regexp_instr(note_body2, '[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis') > 0
        or regexp_instr(note_body2, '[uU]rosepsis|[hH]epatic [aA]bscess') > 0
        or regexp_instr(note_body2, '([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia') > 0
        or regexp_instr(note_body2, '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)') > 0
        or regexp_instr(note_body2, '[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis') > 0
        or regexp_instr(note_body2, '[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*') > 0
        or regexp_instr(note_body2, '[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis') > 0
        or regexp_instr(note_body2, '[pP]neumotosis| CAP |CLABSI') > 0

        or regexp_instr(note_body3, '[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis') > 0
        or regexp_instr(note_body3, '[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis') > 0
        or regexp_instr(note_body3, '[uU]rosepsis|[hH]epatic [aA]bscess') > 0
        or regexp_instr(note_body3, '([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia') > 0
        or regexp_instr(note_body3, '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)') > 0
        or regexp_instr(note_body3, '[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis') > 0
        or regexp_instr(note_body3, '[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*') > 0
        or regexp_instr(note_body3, '[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis') > 0
        or regexp_instr(note_body3, '[pP]neumotosis| CAP |CLABSI') > 0
      )
) R
;






create table cdm_processed_notes_d11 as
select dataset_id, pat_id, note_id, note_type, note_status, start_ts,
       doc2ngrams(note_body1) as ngrams1,
       doc2ngrams(note_body2) as ngrams2,
       doc2ngrams(note_body3) as ngrams3
from (
select  dataset_id, pat_id, note_id, note_type, note_status, start_ts,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(note_body1, '([bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis)', '##**\\1**##')
                                 , '([uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis)', '##**\\1**##')
                                 , '([uU]rosepsis|[hH]epatic [aA]bscess)', '##**\\1**##')
                                 , '(([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia)', '##**\\1**##')
                                 , '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)', '##**\\1**##')
                                 , '([iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis)', '##**\\1**##')
                                 , '([lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*)', '##**\\1**##')
                                 , '([iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis)', '##**\\1**##')
                                 , '([pP]neumotosis| CAP |CLABSI)', '##**\\1**##')
          as note_body1,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(note_body2, '([bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis)', '##**\\1**##')
                                 , '([uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis)', '##**\\1**##')
                                 , '([uU]rosepsis|[hH]epatic [aA]bscess)', '##**\\1**##')
                                 , '(([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia)', '##**\\1**##')
                                 , '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)', '##**\\1**##')
                                 , '([iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis)', '##**\\1**##')
                                 , '([lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*)', '##**\\1**##')
                                 , '([iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis)', '##**\\1**##')
                                 , '([pP]neumotosis| CAP |CLABSI)', '##**\\1**##')
          as note_body2,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(note_body3, '([bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis)', '##**\\1**##')
                                 , '([uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis)', '##**\\1**##')
                                 , '([uU]rosepsis|[hH]epatic [aA]bscess)', '##**\\1**##')
                                 , '(([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia)', '##**\\1**##')
                                 , '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)', '##**\\1**##')
                                 , '([iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis)', '##**\\1**##')
                                 , '([lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*)', '##**\\1**##')
                                 , '([iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis)', '##**\\1**##')
                                 , '([pP]neumotosis| CAP |CLABSI)', '##**\\1**##')
          as note_body3
from (
select  N.dataset_id, N.pat_id, N.note_id, N.note_type, N.note_status,
        json_extract_path_text(N.dates, 'entry_instant_dttm')::timestamptz as start_ts,
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(N.note_body1, '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rosepsis|[hH]epatic [aA]bscess', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[pP]neumotosis| CAP |CLABSI', 'NEGATED_PHRASE')
          as note_body1,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(N.note_body2, '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rosepsis|[hH]epatic [aA]bscess', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[pP]neumotosis| CAP |CLABSI', 'NEGATED_PHRASE')
          as note_body2,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(N.note_body3, '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rosepsis|[hH]epatic [aA]bscess', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[pP]neumotosis| CAP |CLABSI', 'NEGATED_PHRASE')
          as note_body3


from (
  select N.dataset_id, N.pat_id, N.note_id, N.note_type, N.note_status, N.dates,
         substring(N.note_body1 for 63035) as note_body1,
         substring(N.note_body1 from 63036 for 2500) || substring(N.note_body2 for 60035) as note_body2,
         substring(N.note_body2 from 60036 for 5000) || N.note_body3 as note_body3
  from cdm_notes N
  where N.dataset_id = 11
  and N.author_type <> 'Pharmacist'
) N
) NEG
where (
           regexp_instr(note_body1, '[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis') > 0
        or regexp_instr(note_body1, '[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis') > 0
        or regexp_instr(note_body1, '[uU]rosepsis|[hH]epatic [aA]bscess') > 0
        or regexp_instr(note_body1, '([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia') > 0
        or regexp_instr(note_body1, '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)') > 0
        or regexp_instr(note_body1, '[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis') > 0
        or regexp_instr(note_body1, '[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*') > 0
        or regexp_instr(note_body1, '[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis') > 0
        or regexp_instr(note_body1, '[pP]neumotosis| CAP |CLABSI') > 0

        or regexp_instr(note_body2, '[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis') > 0
        or regexp_instr(note_body2, '[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis') > 0
        or regexp_instr(note_body2, '[uU]rosepsis|[hH]epatic [aA]bscess') > 0
        or regexp_instr(note_body2, '([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia') > 0
        or regexp_instr(note_body2, '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)') > 0
        or regexp_instr(note_body2, '[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis') > 0
        or regexp_instr(note_body2, '[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*') > 0
        or regexp_instr(note_body2, '[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis') > 0
        or regexp_instr(note_body2, '[pP]neumotosis| CAP |CLABSI') > 0

        or regexp_instr(note_body3, '[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis') > 0
        or regexp_instr(note_body3, '[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis') > 0
        or regexp_instr(note_body3, '[uU]rosepsis|[hH]epatic [aA]bscess') > 0
        or regexp_instr(note_body3, '([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia') > 0
        or regexp_instr(note_body3, '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)') > 0
        or regexp_instr(note_body3, '[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis') > 0
        or regexp_instr(note_body3, '[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*') > 0
        or regexp_instr(note_body3, '[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis') > 0
        or regexp_instr(note_body3, '[pP]neumotosis| CAP |CLABSI') > 0
      )
) R
;





create table cdm_processed_notes_d12 as
select dataset_id, pat_id, note_id, note_type, note_status, start_ts,
       doc2ngrams(note_body1) as ngrams1,
       doc2ngrams(note_body2) as ngrams2,
       doc2ngrams(note_body3) as ngrams3
from (
select  dataset_id, pat_id, note_id, note_type, note_status, start_ts,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(note_body1, '([bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis)', '##**\\1**##')
                                 , '([uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis)', '##**\\1**##')
                                 , '([uU]rosepsis|[hH]epatic [aA]bscess)', '##**\\1**##')
                                 , '(([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia)', '##**\\1**##')
                                 , '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)', '##**\\1**##')
                                 , '([iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis)', '##**\\1**##')
                                 , '([lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*)', '##**\\1**##')
                                 , '([iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis)', '##**\\1**##')
                                 , '([pP]neumotosis| CAP |CLABSI)', '##**\\1**##')
          as note_body1,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(note_body2, '([bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis)', '##**\\1**##')
                                 , '([uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis)', '##**\\1**##')
                                 , '([uU]rosepsis|[hH]epatic [aA]bscess)', '##**\\1**##')
                                 , '(([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia)', '##**\\1**##')
                                 , '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)', '##**\\1**##')
                                 , '([iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis)', '##**\\1**##')
                                 , '([lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*)', '##**\\1**##')
                                 , '([iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis)', '##**\\1**##')
                                 , '([pP]neumotosis| CAP |CLABSI)', '##**\\1**##')
          as note_body2,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(note_body3, '([bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis)', '##**\\1**##')
                                 , '([uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis)', '##**\\1**##')
                                 , '([uU]rosepsis|[hH]epatic [aA]bscess)', '##**\\1**##')
                                 , '(([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia)', '##**\\1**##')
                                 , '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)', '##**\\1**##')
                                 , '([iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis)', '##**\\1**##')
                                 , '([lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*)', '##**\\1**##')
                                 , '([iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis)', '##**\\1**##')
                                 , '([pP]neumotosis| CAP |CLABSI)', '##**\\1**##')
          as note_body3
from (
select  N.dataset_id, N.pat_id, N.note_id, N.note_type, N.note_status,
        json_extract_path_text(N.dates, 'entry_instant_dttm')::timestamptz as start_ts,
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(N.note_body1, '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rosepsis|[hH]epatic [aA]bscess', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[pP]neumotosis| CAP |CLABSI', 'NEGATED_PHRASE')
          as note_body1,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(N.note_body2, '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rosepsis|[hH]epatic [aA]bscess', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[pP]neumotosis| CAP |CLABSI', 'NEGATED_PHRASE')
          as note_body2,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(N.note_body3, '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rosepsis|[hH]epatic [aA]bscess', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[pP]neumotosis| CAP |CLABSI', 'NEGATED_PHRASE')
          as note_body3


from (
  select N.dataset_id, N.pat_id, N.note_id, N.note_type, N.note_status, N.dates,
         substring(N.note_body1 for 63035) as note_body1,
         substring(N.note_body1 from 63036 for 2500) || substring(N.note_body2 for 60035) as note_body2,
         substring(N.note_body2 from 60036 for 5000) || N.note_body3 as note_body3
  from cdm_notes N
  where N.dataset_id = 12
  and N.author_type <> 'Pharmacist'
) N
) NEG
where (
           regexp_instr(note_body1, '[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis') > 0
        or regexp_instr(note_body1, '[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis') > 0
        or regexp_instr(note_body1, '[uU]rosepsis|[hH]epatic [aA]bscess') > 0
        or regexp_instr(note_body1, '([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia') > 0
        or regexp_instr(note_body1, '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)') > 0
        or regexp_instr(note_body1, '[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis') > 0
        or regexp_instr(note_body1, '[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*') > 0
        or regexp_instr(note_body1, '[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis') > 0
        or regexp_instr(note_body1, '[pP]neumotosis| CAP |CLABSI') > 0

        or regexp_instr(note_body2, '[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis') > 0
        or regexp_instr(note_body2, '[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis') > 0
        or regexp_instr(note_body2, '[uU]rosepsis|[hH]epatic [aA]bscess') > 0
        or regexp_instr(note_body2, '([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia') > 0
        or regexp_instr(note_body2, '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)') > 0
        or regexp_instr(note_body2, '[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis') > 0
        or regexp_instr(note_body2, '[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*') > 0
        or regexp_instr(note_body2, '[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis') > 0
        or regexp_instr(note_body2, '[pP]neumotosis| CAP |CLABSI') > 0

        or regexp_instr(note_body3, '[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis') > 0
        or regexp_instr(note_body3, '[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis') > 0
        or regexp_instr(note_body3, '[uU]rosepsis|[hH]epatic [aA]bscess') > 0
        or regexp_instr(note_body3, '([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia') > 0
        or regexp_instr(note_body3, '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)') > 0
        or regexp_instr(note_body3, '[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis') > 0
        or regexp_instr(note_body3, '[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*') > 0
        or regexp_instr(note_body3, '[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis') > 0
        or regexp_instr(note_body3, '[pP]neumotosis| CAP |CLABSI') > 0
      )
) R
;





create table cdm_processed_notes_d13 as
select dataset_id, pat_id, note_id, note_type, note_status, start_ts,
       doc2ngrams(note_body1) as ngrams1,
       doc2ngrams(note_body2) as ngrams2,
       doc2ngrams(note_body3) as ngrams3
from (
select  dataset_id, pat_id, note_id, note_type, note_status, start_ts,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(note_body1, '([bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis)', '##**\\1**##')
                                 , '([uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis)', '##**\\1**##')
                                 , '([uU]rosepsis|[hH]epatic [aA]bscess)', '##**\\1**##')
                                 , '(([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia)', '##**\\1**##')
                                 , '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)', '##**\\1**##')
                                 , '([iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis)', '##**\\1**##')
                                 , '([lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*)', '##**\\1**##')
                                 , '([iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis)', '##**\\1**##')
                                 , '([pP]neumotosis| CAP |CLABSI)', '##**\\1**##')
          as note_body1,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(note_body2, '([bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis)', '##**\\1**##')
                                 , '([uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis)', '##**\\1**##')
                                 , '([uU]rosepsis|[hH]epatic [aA]bscess)', '##**\\1**##')
                                 , '(([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia)', '##**\\1**##')
                                 , '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)', '##**\\1**##')
                                 , '([iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis)', '##**\\1**##')
                                 , '([lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*)', '##**\\1**##')
                                 , '([iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis)', '##**\\1**##')
                                 , '([pP]neumotosis| CAP |CLABSI)', '##**\\1**##')
          as note_body2,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(note_body3, '([bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis)', '##**\\1**##')
                                 , '([uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis)', '##**\\1**##')
                                 , '([uU]rosepsis|[hH]epatic [aA]bscess)', '##**\\1**##')
                                 , '(([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia)', '##**\\1**##')
                                 , '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)', '##**\\1**##')
                                 , '([iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis)', '##**\\1**##')
                                 , '([lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*)', '##**\\1**##')
                                 , '([iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis)', '##**\\1**##')
                                 , '([pP]neumotosis| CAP |CLABSI)', '##**\\1**##')
          as note_body3
from (
select  N.dataset_id, N.pat_id, N.note_id, N.note_type, N.note_status,
        json_extract_path_text(N.dates, 'entry_instant_dttm')::timestamptz as start_ts,
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(N.note_body1, '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rosepsis|[hH]epatic [aA]bscess', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[pP]neumotosis| CAP |CLABSI', 'NEGATED_PHRASE')
          as note_body1,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(N.note_body2, '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rosepsis|[hH]epatic [aA]bscess', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[pP]neumotosis| CAP |CLABSI', 'NEGATED_PHRASE')
          as note_body2,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(N.note_body3, '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rosepsis|[hH]epatic [aA]bscess', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[pP]neumotosis| CAP |CLABSI', 'NEGATED_PHRASE')
          as note_body3


from (
  select N.dataset_id, N.pat_id, N.note_id, N.note_type, N.note_status, N.dates,
         substring(N.note_body1 for 63035) as note_body1,
         substring(N.note_body1 from 63036 for 2500) || substring(N.note_body2 for 60035) as note_body2,
         substring(N.note_body2 from 60036 for 5000) || N.note_body3 as note_body3
  from cdm_notes N
  where N.dataset_id = 13
  and N.author_type <> 'Pharmacist'
) N
) NEG
where (
           regexp_instr(note_body1, '[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis') > 0
        or regexp_instr(note_body1, '[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis') > 0
        or regexp_instr(note_body1, '[uU]rosepsis|[hH]epatic [aA]bscess') > 0
        or regexp_instr(note_body1, '([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia') > 0
        or regexp_instr(note_body1, '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)') > 0
        or regexp_instr(note_body1, '[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis') > 0
        or regexp_instr(note_body1, '[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*') > 0
        or regexp_instr(note_body1, '[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis') > 0
        or regexp_instr(note_body1, '[pP]neumotosis| CAP |CLABSI') > 0

        or regexp_instr(note_body2, '[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis') > 0
        or regexp_instr(note_body2, '[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis') > 0
        or regexp_instr(note_body2, '[uU]rosepsis|[hH]epatic [aA]bscess') > 0
        or regexp_instr(note_body2, '([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia') > 0
        or regexp_instr(note_body2, '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)') > 0
        or regexp_instr(note_body2, '[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis') > 0
        or regexp_instr(note_body2, '[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*') > 0
        or regexp_instr(note_body2, '[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis') > 0
        or regexp_instr(note_body2, '[pP]neumotosis| CAP |CLABSI') > 0

        or regexp_instr(note_body3, '[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis') > 0
        or regexp_instr(note_body3, '[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis') > 0
        or regexp_instr(note_body3, '[uU]rosepsis|[hH]epatic [aA]bscess') > 0
        or regexp_instr(note_body3, '([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia') > 0
        or regexp_instr(note_body3, '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)') > 0
        or regexp_instr(note_body3, '[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis') > 0
        or regexp_instr(note_body3, '[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*') > 0
        or regexp_instr(note_body3, '[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis') > 0
        or regexp_instr(note_body3, '[pP]neumotosis| CAP |CLABSI') > 0
      )
) R
;


create table cdm_processed_notes_d3 as
select dataset_id, pat_id, note_id, note_type, note_status, start_ts,
       doc2ngrams(note_body1) as ngrams1,
       doc2ngrams(note_body2) as ngrams2,
       doc2ngrams(note_body3) as ngrams3
from (
select  dataset_id, pat_id, note_id, note_type, note_status, start_ts,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(note_body1, '([bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis)', '##**\\1**##')
                                 , '([uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis)', '##**\\1**##')
                                 , '([uU]rosepsis|[hH]epatic [aA]bscess)', '##**\\1**##')
                                 , '(([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia)', '##**\\1**##')
                                 , '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)', '##**\\1**##')
                                 , '([iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis)', '##**\\1**##')
                                 , '([lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*)', '##**\\1**##')
                                 , '([iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis)', '##**\\1**##')
                                 , '([pP]neumotosis| CAP |CLABSI)', '##**\\1**##')
          as note_body1,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(note_body2, '([bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis)', '##**\\1**##')
                                 , '([uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis)', '##**\\1**##')
                                 , '([uU]rosepsis|[hH]epatic [aA]bscess)', '##**\\1**##')
                                 , '(([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia)', '##**\\1**##')
                                 , '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)', '##**\\1**##')
                                 , '([iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis)', '##**\\1**##')
                                 , '([lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*)', '##**\\1**##')
                                 , '([iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis)', '##**\\1**##')
                                 , '([pP]neumotosis| CAP |CLABSI)', '##**\\1**##')
          as note_body2,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(note_body3, '([bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis)', '##**\\1**##')
                                 , '([uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis)', '##**\\1**##')
                                 , '([uU]rosepsis|[hH]epatic [aA]bscess)', '##**\\1**##')
                                 , '(([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia)', '##**\\1**##')
                                 , '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)', '##**\\1**##')
                                 , '([iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis)', '##**\\1**##')
                                 , '([lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*)', '##**\\1**##')
                                 , '([iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis)', '##**\\1**##')
                                 , '([pP]neumotosis| CAP |CLABSI)', '##**\\1**##')
          as note_body3
from (
select  N.dataset_id, N.pat_id, N.note_id, N.note_type, N.note_status,
        json_extract_path_text(N.dates, 'entry_instant_dttm')::timestamptz as start_ts,
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(N.note_body1, '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rosepsis|[hH]epatic [aA]bscess', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[pP]neumotosis| CAP |CLABSI', 'NEGATED_PHRASE')
          as note_body1,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(N.note_body2, '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rosepsis|[hH]epatic [aA]bscess', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[pP]neumotosis| CAP |CLABSI', 'NEGATED_PHRASE')
          as note_body2,

        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(N.note_body3, '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[uU]rosepsis|[hH]epatic [aA]bscess', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis', 'NEGATED_PHRASE')
                                   , '([hH](istory)*(x)* of( recurrent|recent)*|[hH]/[oO]|[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*|[nN]ot|[Nn]eg(ative)*|[rR]ecently had|[dD]enies|[dD]enies any|[rR]ecently hospitalized for|[rR]ecurrent|[aA]bsence of|[hH]ave had|•[ ]*)[pP]neumotosis| CAP |CLABSI', 'NEGATED_PHRASE')
          as note_body3


from (
  select N.dataset_id, N.pat_id, N.note_id, N.note_type, N.note_status, N.dates,
         substring(N.note_body1 for 63035) as note_body1,
         substring(N.note_body1 from 63036 for 2500) || substring(N.note_body2 for 60035) as note_body2,
         substring(N.note_body2 from 60036 for 5000) || N.note_body3 as note_body3
  from cdm_notes N
  where N.dataset_id = 3
  and N.author_type <> 'Pharmacist'
) N
) NEG
where (
           regexp_instr(note_body1, '[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis') > 0
        or regexp_instr(note_body1, '[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis') > 0
        or regexp_instr(note_body1, '[uU]rosepsis|[hH]epatic [aA]bscess') > 0
        or regexp_instr(note_body1, '([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia') > 0
        or regexp_instr(note_body1, '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)') > 0
        or regexp_instr(note_body1, '[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis') > 0
        or regexp_instr(note_body1, '[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*') > 0
        or regexp_instr(note_body1, '[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis') > 0
        or regexp_instr(note_body1, '[pP]neumotosis| CAP |CLABSI') > 0

        or regexp_instr(note_body2, '[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis') > 0
        or regexp_instr(note_body2, '[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis') > 0
        or regexp_instr(note_body2, '[uU]rosepsis|[hH]epatic [aA]bscess') > 0
        or regexp_instr(note_body2, '([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia') > 0
        or regexp_instr(note_body2, '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)') > 0
        or regexp_instr(note_body2, '[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis') > 0
        or regexp_instr(note_body2, '[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*') > 0
        or regexp_instr(note_body2, '[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis') > 0
        or regexp_instr(note_body2, '[pP]neumotosis| CAP |CLABSI') > 0

        or regexp_instr(note_body3, '[bB]iliary [sS]epsis|[pP]neumoperitoneum| HCAP |[cC]entral [lL]ine [iI]nfection|[cC]ystitis|[pP]eritonitis|[eE]mpyema|[mM]yocarditis') > 0
        or regexp_instr(note_body3, '[uU]rinary [tT]ract [iI]nfection|[cC]holangitis|(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*|[eE]ndocarditis|[nN]ecrotizing [fF]asciitis') > 0
        or regexp_instr(note_body3, '[uU]rosepsis|[hH]epatic [aA]bscess') > 0
        or regexp_instr(note_body3, '([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia') > 0
        or regexp_instr(note_body3, '(([rR]ight[ ]*|[lL]eft[ ]*|[lL]ower[ ]*|[eE]xtremity[ ]*)*[cC]ellulitis)') > 0
        or regexp_instr(note_body3, '[iI]nfluenza|(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])|[aA]ppendicitis|[sS]pontaneous [bB]acerial [pP]eritonitis') > 0
        or regexp_instr(note_body3, '[lL]ine [sS]epsis|[oO]steomyelitis| CAUTI |[cC]holelithiasis|[cC]olitis|[bB]lood [sS]tream [iI]nfection|[pP]seudomonas|[pP]yelo(nephritis)*') > 0
        or regexp_instr(note_body3, '[iI]ntra [aA]bdominal [sS]epsis|[^a-zA-Z]PNA[^a-zA-Z]|[mM]ediastinitis|([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess|[cC]holecystitis') > 0
        or regexp_instr(note_body3, '[pP]neumotosis| CAP |CLABSI') > 0
      )
) R
;
