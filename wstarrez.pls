CREATE OR REPLACE PACKAGE                                                "WSTARREZ" is 


  procedure p_main(pv_term_code varchar2 , pv_date varchar2 , pv_delim varchar2 default chr(9) );
  procedure p_write_file_demo (pv_term_code varchar2, pv_date varchar2);
  function f_get_addr(pv_pidm number , pv_atyp varchar2, pv_addr_field varchar2) return varchar2;
  PROCEDURE          p_write_wbbphot(pv_student_id spriden.spriden_id%type , pv_refresh_date darton.WBBPHOT.CREATE_DATE%type default sysdate);
  procedure p_get_bookings;
  procedure p_update_assignment(pv_pidm spriden.spriden_pidm%type, pv_ascd_code stvascd.stvascd_code%type , pv_term_code stvterm.stvterm_code%type);
  procedure p_insert_assignment(pv_pidm spriden.spriden_pidm%type, pv_bldg_code slbrdef.slbrdef_bldg_code%type, pv_room_number slbrdef.slbrdef_room_number%type, pv_term_code stvterm.stvterm_code%type,
                            pv_begin_date slrrasg.slrrasg_begin_date%type, pv_end_date slrrasg.slrrasg_end_date%type , pv_ascd_code slrrasg.slrrasg_ascd_code%type);
  procedure p_get_meals;
  end wstarrez;
/


CREATE OR REPLACE PACKAGE BODY                                                                                             WSTARREZ is 
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
   
  curr_release constant VARCHAR2(10) := '6.1';
  gv_delim varchar2(10);
  gv_file_dir varchar2(255) := 'DATA_OUT';
  gv_data_origin varchar2(15) := 'SR_BOOKING';
  gv_user_id varchar2(15) := 'STARREZ';
  gv_assess_charges varchar2(1) := 'N';
  gv_ar_ind varchar2(1) := 'N'; 
  
  procedure p_main(pv_term_code varchar2 , pv_date varchar2 , pv_delim varchar2 default chr(9) ) is
  
  begin
  DBMS_OUTPUT.ENABLE (buffer_size => NULL); --Ensure no limit on buffer output size
  gv_delim := pv_delim; --delimiter for all output files
  --p_write_file_demo (pv_term_code, pv_date);
   p_get_bookings;
   p_get_meals;
  end p_main;
  
--Generates the StarRez Standard Demographic Import Interface from Banner
procedure  p_write_file_demo(pv_term_code varchar2, pv_date varchar2) IS

 CURSOR c_pop_sel IS
SELECT
  
  spriden_id student_id,  
  spriden_pidm banner_id, 
  gobtpac_external_user username,                          --3rd party id
  
  spbpers_name_prefix prefix,                         
  spriden_last_name last_name,                             
  spriden_first_name first_name,                          
  substr(spriden_mi,0,1)  middle_initial,                     
  spbpers_pref_first_name preferred_first_name,               
  
  DECODE(spbpers_sex, 'M', '1', 'F', '2', '0') sex,       
  TO_CHAR(spbpers_birth_date, 'MM/DD/YYYY') dob,    
  
  DECODE(spbpers_mrtl_code, 'M', '1', '0') marital_status, 
  zobresi_natn_code  nationality, 
  (SELECT stvethn_desc  FROM stvethn  WHERE  stvethn_code = spbpers_ethn_code )  ethn,  
  
  DECODE(spbpers_confid_ind, 'Y', '1', '0') directory_info_privacy_flag, 
  bwwkstud.f_get_email(spriden_pidm, 'MYDC') email_address,   
  bwwkstud.f_get_email(spriden_pidm, 'PERS') alt_email_address,   
  
  bwwkstud.f_get_stustreet1(spriden_pidm) mailing_address_street1,          
  bwwkstud.f_get_stustreet2(spriden_pidm) mailing_address_street2,      
  bwwkstud.f_get_stucity(spriden_pidm) mailing_address_city,            
  bwwkstud.f_get_stustatecd(spriden_pidm) mailing_address_state,      
  bwwkstud.f_get_stuzip(spriden_pidm) mailing_address_zip,          
  bwwkstud.f_get_stunationcode(spriden_pidm) mailing_address_country_code,      
  
  bwwkstud.f_get_phone(spriden_pidm, 'MA') mailing_phone,       
  bwwkstud.f_get_phone(spriden_pidm, 'CELL') mailing_cell_phone, 
  
  f_get_addr(spriden_pidm, 'PR', 'STR1') home_address_street1,        
  f_get_addr(spriden_pidm, 'PR', 'STR2') home_address_street2, 
  f_get_addr(spriden_pidm, 'PR', 'CITY')  home_address_city, 
  f_get_addr(spriden_pidm, 'PR', 'STATE')  home_address_state,
  f_get_addr(spriden_pidm, 'PR', 'ZIP')  home_address_zip, 
  f_get_addr(spriden_pidm, 'PR', 'NATN')  home_address_country_code, 
  
  ---Emergency contact
  (    SELECT      spremrg_first_name      || ' '      || spremrg_mi      || ' '      || spremrg_last_name em_name    FROM      spremrg    WHERE      spriden_pidm       = spremrg_pidm    AND spremrg_priority = 1  )  emergency_contact,
  (    SELECT      spremrg_street_line1    FROM      spremrg    WHERE      spriden_pidm       = spremrg_pidm    AND spremrg_priority = 1  )  emergency_street1,
  (    SELECT      spremrg_street_line2    FROM      spremrg    WHERE      spriden_pidm       = spremrg_pidm    AND spremrg_priority = 1  )  emergency_street2,
  (    SELECT      spremrg_city    FROM      spremrg    WHERE      spriden_pidm       = spremrg_pidm    AND spremrg_priority = 1  )  emergency_city,
  (    SELECT      spremrg_stat_code    FROM      spremrg    WHERE      spriden_pidm       = spremrg_pidm    AND spremrg_priority = 1  )  emergency_state,
  (    SELECT      spremrg_zip    FROM      spremrg    WHERE      spriden_pidm       = spremrg_pidm    AND spremrg_priority = 1  )  emergency_zip,
  (    SELECT      '('      || spremrg_phone_area      || ')'      || spremrg_phone_number    FROM      spremrg    WHERE      spriden_pidm       = spremrg_pidm    AND spremrg_priority = 1  )  emergency_phone,
  (    SELECT      stvrelt_desc     FROM      spremrg, stvrelt    WHERE      spriden_pidm       = spremrg_pidm    AND spremrg_priority = 1 and spremrg_relt_code = stvrelt_code  )  emergency_relationship,
  (    SELECT      spremrg_natn_code    FROM      spremrg    WHERE      spriden_pidm       = spremrg_pidm    AND spremrg_priority = 1  )  emergency_natn,
  --Student status
  DECODE(student.f_get_class_ind (spriden_pidm, pv_term_code), 'F', 'Freshman'  , 'S', 'Sophomore') student_classification,
  (    SELECT      sgbstdn_levl_code    FROM      sgbstdn    WHERE      sgbstdn_pidm            = spriden_pidm    AND sgbstdn_term_code_eff =      (        SELECT          MAX(z.sgbstdn_term_code_eff)
        FROM          sgbstdn z        WHERE          z.sgbstdn_pidm = spriden_pidm      )  )  student_level,
  (    SELECT      sgbstdn_styp_code    FROM      sgbstdn    WHERE      sgbstdn_pidm            = spriden_pidm    AND sgbstdn_term_code_eff =      
      (        SELECT          MAX(z.sgbstdn_term_code_eff)        FROM          sgbstdn z        WHERE          z.sgbstdn_pidm = spriden_pidm      )  )  student_type, 
  (    SELECT      sgbstdn_term_code_admit    FROM      sgbstdn    WHERE      sgbstdn_pidm            = spriden_pidm    AND sgbstdn_term_code_eff =
      (        SELECT          MAX(z.sgbstdn_term_code_eff)        FROM          sgbstdn z        WHERE          z.sgbstdn_pidm = spriden_pidm      )  )  admit_term,
  
  ---Holds
  NVL (  (    SELECT DISTINCT      '1'    FROM      sprhold,      stvhldd    WHERE      spriden_pidm           = sprhold_pidm    AND sprhold_hldd_code    = stvhldd_code
          AND stvhldd_reg_hold_ind = 'Y'    AND sysdate BETWEEN sprhold_from_date AND sprhold_to_date    AND sprhold_hldd_code <> 'WR'  )  , '0') academic_hold,
  decode(nvl(bwwkstud.F_hold_exists (spriden_pidm,'VP'),0),'Y','1','0') judicial_hold, 
  NVL (  (    SELECT DISTINCT      '1'    FROM      sprhold,      stvhldd    WHERE      spriden_pidm          = sprhold_pidm    AND sprhold_hldd_code   = stvhldd_code    AND stvhldd_ar_hold_ind = 'Y'
    AND sysdate BETWEEN sprhold_from_date AND sprhold_to_date  )  , '0') financial_hold,
  
  --Athlete
  decode(bwwkstud.f_athletic_participant(spriden_pidm, pv_term_code),'Y',1,0) athlete_code,
  (select stvactc_desc from stvactc where stvactc_code = bwwkstud.f_get_sport(spriden_pidm, pv_term_code)) athlete_team,
  bwwkstud.f_get_stumajordesc(spriden_pidm, pv_term_code) major,
  
  --Hours/GPA
  bwwkstud.f_get_curr_bill_hrs(spriden_pidm, pv_term_code) hours_enrolled,
  (select shrlgpa_hours_earned from shrlgpa where shrlgpa_pidm = spriden_pidm and shrlgpa_gpa_type_ind = 'I' and shrlgpa_levl_code = 'US') hours_complete,
  (select shrlgpa_hours_attempted from shrlgpa where shrlgpa_pidm = spriden_pidm and shrlgpa_gpa_type_ind = 'I' and shrlgpa_levl_code = 'US') hours_attempted,
  (select trunc(shrlgpa_gpa, 2) from shrlgpa where shrlgpa_pidm = spriden_pidm and shrlgpa_gpa_type_ind = 'I' and shrlgpa_levl_code = 'US') gpa, 
  
  (SELECT      sgbstdn_exp_grad_date    FROM      sgbstdn    WHERE      sgbstdn_pidm            = spriden_pidm    AND sgbstdn_term_code_eff =
      (        SELECT          MAX(z.sgbstdn_term_code_eff)        FROM          sgbstdn z        WHERE          z.sgbstdn_pidm = spriden_pidm      )  ) expected_graduation_date,
  (    SELECT      DECODE(sgbstdn_resd_code, 'R', '1', '0')    FROM      sgbstdn    WHERE      sgbstdn_pidm            = spriden_pidm    AND sgbstdn_term_code_eff =
      (        SELECT          MAX(z.sgbstdn_term_code_eff)        FROM          sgbstdn z        WHERE          z.sgbstdn_pidm = spriden_pidm      )  ) residency,
  (    SELECT      stvstyp_desc    FROM      sgbstdn, stvstyp    WHERE      sgbstdn_pidm            = spriden_pidm    and sgbstdn_styp_code = stvstyp_code AND sgbstdn_term_code_eff =      
      (        SELECT          MAX(z.sgbstdn_term_code_eff)        FROM          sgbstdn z        WHERE          z.sgbstdn_pidm = spriden_pidm      )  ) classification,

-- Requested additions:
-- Account Balance (all tutition and fees to be paid)
    (select sum(tbraccd_balance) from tbraccd where tbraccd_term_code = pv_term_code and tbraccd_pidm = spriden_pidm group by tbraccd_pidm) acct_balance,
-- International Student indicator
NVL((select '1' from gorvisa where gorvisa_pidm = spriden_pidm and gorvisa_vtyp_code = 'F1'), '0') intl_ind,
-- Faculty/Staff indicator
    NVL((select '1' from warshep, wtvstat where warshep_pidm = spriden_pidm and warshep_stat = wtvstat_code and wtvstat_emp_ind = 'Y'), '0') fac_staff_ind

  FROM
    spriden,
    spbpers,
    gobtpac,
    zobresi
  WHERE
    spriden_change_ind IS NULL
    and zobresi_pidm = spriden_pidm
  
AND spriden_pidm      = spbpers_pidm
AND gobtpac_pidm = spriden_pidm
AND (sb_enrollment.f_exists(spriden_pidm, pv_term_code) = 'Y'
    OR EXISTS (select 'X' from saradap where saradap_pidm = spriden_pidm
              and saradap_term_code_entry >= pv_term_code
              and saradap_apst_code = 'D'
              )
    ) 
;

  lv_rpt c_pop_sel%rowtype;
  lv_report_file    UTL_FILE.FILE_TYPE;
  lv_chklst_desc varchar2(2000);
   
  BEGIN

    --lv_report_file := utl_file.fopen(gv_file_dir, pv_date || '_DemographicImportBanner.txt', 'W');
lv_report_file := utl_file.fopen(gv_file_dir, 'DemographicImportBanner.txt', 'W');

  UTL_FILE.Put_Line (lv_report_file,
     'STUDENT_ID'|| gv_delim ||
     'BANNER_ID'|| gv_delim ||
     'USERNAME' ||gv_delim ||
     'NAME_PREFIX'|| gv_delim ||
     'LAST_NAME'|| gv_delim ||
     'FIRST_NAME'|| gv_delim ||
     'MIDDLE_NAME'|| gv_delim ||
     'PREF_FIRST_NAME'|| gv_delim ||
     'SEX'|| gv_delim ||
     'BIRTHDATE'|| gv_delim ||
     'MARITAL_STATUS'|| gv_delim ||
     'NATN'|| gv_delim ||
     'ETHN'|| gv_delim ||
     'DIRECTORY_FLAG_IND'|| gv_delim ||
     'EMAIL_ADDRESS_1'|| gv_delim ||
     'EMAIL_ADDRESS_2'|| gv_delim ||
     'MA_STREET1'|| gv_delim ||
     'MA_STREET2'|| gv_delim ||
     'MA_CITY'|| gv_delim ||
     'MA_STATE'|| gv_delim ||
     'MA_ZIP'|| gv_delim ||
     'MA_NATN'|| gv_delim ||
     'MA_PHONE'|| gv_delim ||
     'MA_CELL_PHONE'|| gv_delim ||
     'PR_STREET1'|| gv_delim ||
     'PR_STREET2'|| gv_delim ||
     'PR_CITY'|| gv_delim ||
     'PR_STATE'|| gv_delim ||
     'PR_ZIP'|| gv_delim ||
     'PR_NATN'|| gv_delim ||
     'EM_CONTACT'|| gv_delim ||
     'EM_STREET1'|| gv_delim ||
     'EM_STREET2'|| gv_delim ||
     'EM_CITY'|| gv_delim ||
     'EM_STATE'|| gv_delim ||
     'EM_ZIP'|| gv_delim ||
     'EM_CONTACT_PHONE'|| gv_delim ||
     'EM_RELATION'|| gv_delim ||
     'STUDENT_CLASS'|| gv_delim ||
     'STUDENT_LEVEL'|| gv_delim ||
     'STUDENT_TYPE'|| gv_delim ||
     'ADMIT_TERM'|| gv_delim ||
     'ACADEMIC_HOLD'|| gv_delim ||
     'JUDICIAL_HOLD'|| gv_delim ||
     'FINANCIAL_HOLD'|| gv_delim ||
     'SPORT'|| gv_delim ||
     'SPORT_DESC'|| gv_delim ||
     'MAJOR'|| gv_delim ||
     'HOURS_ENROLLED'|| gv_delim ||
     'HOURS_EARNED'|| gv_delim ||
     'HOURS_ATTEMPTED'|| gv_delim ||
     'GPA'|| gv_delim ||
     'EXP_GRAD_DATE'|| gv_delim ||
     'RESIDENCY'|| gv_delim ||
     'CLASSIFICATION' || gv_delim ||
     'ACCT_BALANCE' || gv_delim ||
     'INTL_IND' || gv_delim ||
     'FAC_STAFF_IND' 

     );

     OPEN c_pop_sel;

    LOOP
      FETCH c_pop_sel
      INTO lv_rpt;
      EXIT
    WHEN(c_pop_sel % NOTFOUND);
      
    UTL_FILE.Put_Line(lv_report_file, 
lv_rpt.student_id|| gv_delim ||  
lv_rpt.banner_id|| gv_delim || 
lv_rpt.username || gv_delim ||
lv_rpt.prefix|| gv_delim ||
lv_rpt.last_name|| gv_delim ||                             
lv_rpt.first_name|| gv_delim ||                          
lv_rpt.middle_initial|| gv_delim ||                     
lv_rpt.preferred_first_name|| gv_delim ||               
lv_rpt.sex|| gv_delim ||       
lv_rpt.dob|| gv_delim ||    
lv_rpt.marital_status|| gv_delim || 
lv_rpt.nationality|| gv_delim || 
lv_rpt.ethn|| gv_delim ||  
lv_rpt.directory_info_privacy_flag|| gv_delim || 
lv_rpt.email_address|| gv_delim ||   
lv_rpt.alt_email_address|| gv_delim ||   
lv_rpt.mailing_address_street1|| gv_delim ||          
lv_rpt.mailing_address_street2|| gv_delim ||      
lv_rpt.mailing_address_city|| gv_delim ||            
lv_rpt.mailing_address_state|| gv_delim ||      
lv_rpt.mailing_address_zip|| gv_delim ||          
lv_rpt.mailing_address_country_code|| gv_delim ||      
lv_rpt.mailing_phone|| gv_delim ||       
lv_rpt.mailing_cell_phone|| gv_delim || 
lv_rpt.home_address_street1|| gv_delim ||   
lv_rpt.home_address_street2|| gv_delim || 
lv_rpt.home_address_city|| gv_delim || 
lv_rpt.home_address_state|| gv_delim ||
lv_rpt.home_address_zip|| gv_delim || 
lv_rpt.home_address_country_code|| gv_delim ||
lv_rpt.emergency_contact|| gv_delim ||
lv_rpt.emergency_street1|| gv_delim ||
lv_rpt.emergency_street2|| gv_delim ||
lv_rpt.emergency_city|| gv_delim ||
lv_rpt.emergency_state|| gv_delim ||
lv_rpt.emergency_zip|| gv_delim ||
lv_rpt.emergency_phone|| gv_delim ||
lv_rpt.emergency_relationship|| gv_delim ||
lv_rpt.student_classification || gv_delim ||
lv_rpt.student_level|| gv_delim ||
lv_rpt.student_type|| gv_delim || 
lv_rpt.admit_term|| gv_delim ||
lv_rpt.academic_hold|| gv_delim ||
lv_rpt.judicial_hold|| gv_delim || 
lv_rpt.financial_hold|| gv_delim ||
lv_rpt.athlete_code|| gv_delim ||
lv_rpt.athlete_team|| gv_delim ||
lv_rpt.major|| gv_delim ||
lv_rpt.hours_enrolled|| gv_delim ||
lv_rpt.hours_complete|| gv_delim ||
lv_rpt.hours_attempted|| gv_delim ||
lv_rpt.gpa|| gv_delim || 
lv_rpt.expected_graduation_date|| gv_delim ||
lv_rpt.residency|| gv_delim ||
lv_rpt.classification || gv_delim ||
     lv_rpt.acct_balance || gv_delim ||
     lv_rpt.intl_ind || gv_delim ||
     lv_rpt.fac_staff_ind
                      );

  p_write_wbbphot(lv_rpt.student_id);  
  
  END LOOP;

  CLOSE c_pop_sel;

  UTL_FILE.FCLOSE(lv_report_file);


END p_write_file_demo;


function f_get_addr(pv_pidm number , pv_atyp varchar2, pv_addr_field varchar2) return varchar2 is
--pv_atyp values are 'PR','MA',etc
lv_str1 spraddr.SPRADDR_STREET_LINE1%TYPE;
lv_str2 SPRADDR.SPRADDR_STREET_LINE2%TYPE;
lv_city SPRADDR.SPRADDR_CITY%TYPE;
lv_state spraddr.SPRADDR_STAT_CODE%TYPE;
lv_zip spraddr.SPRADDR_ZIP%TYPE;
lv_natn spraddr.spraddr_natn_code%TYPE;

lv_result varchar2(100);
begin

 
  SELECT 
      SPRADDR_STREET_LINE1 , SPRADDR_STREET_LINE2, SPRADDR_CITY, SPRADDR_STAT_CODE, SPRADDR_ZIP , spraddr_natn_code
    into lv_str1, lv_str2, lv_city, lv_state, lv_zip , lv_natn
    FROM
      spraddr
    WHERE
      spraddr_pidm        =  pv_pidm
    AND spraddr_atyp_code = pv_atyp
    AND spraddr_seqno     =
      (
        SELECT
          MAX(x.spraddr_seqno)
        FROM
          spraddr x
        WHERE
          x.spraddr_pidm = pv_pidm
      );
  
  case pv_addr_field
    when 'STR1' then lv_result := lv_str1; 
    when 'STR2' then lv_result := lv_str2;
    when 'CITY' then lv_result := lv_city;
    when 'STATE' then lv_result := lv_state;
    when 'ZIP' then lv_result := lv_zip;
    when 'NATN' then lv_result := lv_natn;
    else lv_result := null;
  end case;
  
  return lv_result;

end f_get_addr;



PROCEDURE          p_write_wbbphot(pv_student_id spriden.spriden_id%type , pv_refresh_date darton.WBBPHOT.CREATE_DATE%type default sysdate) is
    v_lob_loc      BLOB;
    v_buffer       RAW(32767);
    v_buffer_size  BINARY_INTEGER;
    v_amount       BINARY_INTEGER;
    v_offset       NUMBER(38) := 1;
    v_chunksize    INTEGER;
    v_out_file     UTL_FILE.FILE_TYPE;
    v_custnum varchar2(9);

BEGIN

  
    -- +-------------------------------------------------------------+
    -- | SELECT THE LOB LOCATOR                                      |
    -- +-------------------------------------------------------------+
   SELECT  photo , custnum
    INTO    v_lob_loc , v_custnum
    FROM    darton.wbbphot , spriden
    WHERE   spriden_change_ind IS NULL
    and wbbphot.custnum = spriden_id 
    and spriden_id = pv_student_id
    and wbbphot.create_date >= pv_refresh_date
    ;

    
    -- +-------------------------------------------------------------+
    -- | FIND OUT THE CHUNKSIZE FOR THIS LOB COLUMN                  |
    -- +-------------------------------------------------------------+
    v_chunksize := DBMS_LOB.GETCHUNKSIZE(v_lob_loc);

    IF (v_chunksize < 32767) THEN
        v_buffer_size := v_chunksize;
    ELSE
        v_buffer_size := 32767;
    END IF;

    v_amount := v_buffer_size;

    -- +-------------------------------------------------------------+
    -- | OPENING THE LOB IS OPTIONAL                                 |
    -- +-------------------------------------------------------------+
   DBMS_LOB.OPEN(v_lob_loc, DBMS_LOB.LOB_READONLY);

    -- +-------------------------------------------------------------+
    -- | WRITE CONTENTS OF THE LOB TO A FILE                         |
    -- +-------------------------------------------------------------+
    v_out_file := UTL_FILE.FOPEN(
        location      => 'DATA_OUT', 
        filename      => v_custnum || '.jpg', 
        open_mode     => 'wb',
        max_linesize  => 32767);

    WHILE v_amount >= v_buffer_size
    LOOP

      DBMS_LOB.READ(
          lob_loc    => v_lob_loc,
          amount     => v_amount,
          offset     => v_offset,
          buffer     => v_buffer);

      v_offset := v_offset + v_amount;

      UTL_FILE.PUT_RAW (
          file      => v_out_file,
          buffer    => v_buffer,
          autoflush => true);

      UTL_FILE.FFLUSH(file => v_out_file);


    END LOOP;

    UTL_FILE.FFLUSH(file => v_out_file);
    UTL_FILE.FCLOSE(v_out_file);
    DBMS_LOB.CLOSE(v_lob_loc);
  exception 
    when NO_DATA_FOUND then return;
    
END p_write_wbbphot;

----------------------------------------------------------------------------------------
/* This procedure will match the records from darton.wsrbook to the appropriate location
management tables for processing. 
*/
----------------------------------------------------------------------------------------

procedure p_get_bookings is

        cursor sr_bookings is
        select 
        wsrbook_pidm,
        wsrbook_id,
            wsrbook_term_code,
              wsrbook_bldg_code,
               wsrbook_room_number,
               wsrbook_begin_date,
               wsrbook_end_date,
               substr(wsrbook_ascd_code,1,4) wsrbook_ascd_code
               --wsrbook_ascd_code
        from   darton.wsrbook;

        ---Student Housing Application
        cursor c_application(pidm_in spriden.spriden_pidm%TYPE,
                        term_in stvterm.stvterm_code%TYPE) IS
        select slbrmap_artp_code,
               slbrmap_to_term,
               slbrmap_prefered_building,
               slbrmap_prefered_room,
               slbrmap_haps_code
        from   slbrmap
        where  slbrmap_pidm = pidm_in
        and    slbrmap_from_term = term_in;
        
        ---Student Housing Assignment
        cursor c_assignment(pidm_in spriden.spriden_pidm%TYPE,
                        term_in stvterm.stvterm_code%TYPE) IS
        select slrrasg_bldg_code,
               slrrasg_room_number
        from   slrrasg
        where  slrrasg_ascd_code = 'AR'
        and    slrrasg_pidm = pidm_in
        and    slrrasg_term_code = term_in;
        
        artp_code varchar2(10);
        to_term   varchar2(6);
        prefered_bldg varchar2(10);
        prefered_room varchar2(10);
        haps_code varchar2(2);
        
        assigned_bldg varchar2(10);
        assigned_room varchar2(10);
        --assign_status varchar2(2);

      ---- Variables to hold output messages ---
      lv_stud_id spriden.spriden_id%type;
      ---Applications
      
      
      --Assignments
      lv_assign_inst_ind varchar2(1);
      lv_room_chg_ind varchar2(1);
      
      
      
  
      begin
      
      dbms_output.put_line('----------- Processing Room Booking Request -----------');
      dbms_output.put_line('Legend: (N)ew, (U)pdate');
      dbms_output.put_line(rpad('STUD ID',11,' ') || rpad('TERM',8,' ') || rpad('BLDG',6,' ') || rpad('RM',6,' ') || rpad('ASCD',6,' ') || rpad('ASSGN',7,' ') || rpad('RM_CHG',8,' ')
                      );
      
      --SLBRMAP -- Meal Plan and Room Assignment Applications
      --SLRRASG -- Room Assignments
      
      --Let's go through each student in the StarRez load file and determine any that do not currenty
      --have an active room assignment; if not then add the room assignment to SLBR

      for booking in sr_bookings
      loop
      
        lv_stud_id := booking.wsrbook_id;  
        
        open  c_application(booking.wsrbook_pidm, booking.wsrbook_term_code);
        fetch c_application into artp_code, to_term, prefered_bldg, prefered_room, haps_code;
        
        if (c_application%NOTFOUND AND booking.wsrbook_ascd_code in ('INRM','RESV') ) then
          --if there is no (housing application) slbrmap record we need to create the application in SLARMAP and create the assignment in SLARASG
            
            begin
                  SAVEPOINT slbrmap_insert;
                  --insert new Housing application with active status
                  insert into slbrmap(slbrmap_pidm,
                                      slbrmap_artp_code,
                                      slbrmap_from_term,
                                      slbrmap_to_term,
                                      slbrmap_appl_priority,
                                      slbrmap_prefered_building,
                                      slbrmap_prefered_room,
                                      slbrmap_prefered_bcat_code,
                                      slbrmap_haps_code,
                                      slbrmap_haps_date,
                                      slbrmap_add_date,
                                      slbrmap_activity_date,
                                      slbrmap_data_origin,
                                      slbrmap_user_id)
                              values (booking.wsrbook_pidm,
                                      'HOME',
                                      booking.wsrbook_term_code,
                                      booking.wsrbook_term_code,
                                      '99999999',
                                      booking.wsrbook_bldg_code,
                                      booking.wsrbook_room_number,
                                      NULL, --bcat_code
                                      'AC',
                                      sysdate,
                                      sysdate,
                                      sysdate,
                                      gv_data_origin,
                                      gv_user_id);
               COMMIT;
               
               exception
               when others then
               dbms_output.put_line('The following Student has errors in load file.');
               ROLLBACK to slbrmap_insert;
               end;
                  
                    lv_assign_inst_ind := 'N';
                    if (booking.wsrbook_ascd_code = 'INRM') then
                    p_insert_assignment(booking.wsrbook_pidm,booking.wsrbook_bldg_code, booking.wsrbook_room_number, booking.wsrbook_term_code,
                                            booking.wsrbook_begin_date,  booking.wsrbook_end_date ,booking.wsrbook_ascd_code);
                  end if;
        else
        
        --Student has an application on file (SLARMAP)
        if (booking.wsrbook_ascd_code in ('INRM','RESV') and haps_code != 'AC')
              OR (booking.wsrbook_ascd_code = 'CNCL' and haps_code = 'AC')
              OR artp_code != 'HOME' then
          --but the imported status does not match the existing status...

            --update the applications (SLARMAP)
            begin
           
           SAVEPOINT slbrmap_update;
           
                update slbrmap
                set    slbrmap_artp_code = 'HOME',
                       slbrmap_to_term = booking.wsrbook_term_code,
                       slbrmap_appl_priority = '99999999',
                       slbrmap_haps_code = decode(booking.wsrbook_ascd_code,'INRM','AC','RESV','RE','CNCL','WD','HIST','IN'),
                       slbrmap_haps_date = sysdate,
                       slbrmap_activity_date = sysdate,
                       slbrmap_data_origin = gv_data_origin,
                       slbrmap_user_id = gv_user_id,
                       slbrmap_prefered_building = booking.wsrbook_bldg_code,
                       slbrmap_prefered_room = booking.wsrbook_room_number
                                         
                where  slbrmap_pidm = booking.wsrbook_pidm
                and    slbrmap_from_term = booking.wsrbook_term_code;
               lv_assign_inst_ind := 'U';
               
               commit;
               
               exception
               when others then
               dbms_output.put_line('The following Student has errors in load file.');
               ROLLBACK to slbrmap_update;
               end;
               
          end if;

          --query for a slrrasg record with AR status...
          open  c_assignment(booking.wsrbook_pidm, booking.wsrbook_term_code);
          fetch c_assignment into assigned_bldg,assigned_room;
          
          --if we have an active housing assignment but the bldg or room does not match and the import record has an active status
          --then this needs to be processed as a room change
          if c_assignment%FOUND then
            if (booking.wsrbook_bldg_code != assigned_bldg or booking.wsrbook_room_number != assigned_room)
               and booking.wsrbook_ascd_code in ('INRM') then

               --update existing records with RC (Room Change) status
               p_update_assignment(booking.wsrbook_pidm , 'RC' , booking.wsrbook_term_code);
               lv_room_chg_ind := 'X';
                
                --and insert a new record with AR status
                  p_insert_assignment(booking.wsrbook_pidm,booking.wsrbook_bldg_code, booking.wsrbook_room_number, booking.wsrbook_term_code,
                                    booking.wsrbook_begin_date,  booking.wsrbook_end_date ,booking.wsrbook_ascd_code);
               
              --Cancelling a booking
              --if the bldg and room does match but the import record has an inactive status
              elsif booking.wsrbook_ascd_code in ('CNCL') then
              
                --update existing records with WR (Withdrawn/Cancelled) status and new slrrasg_end_date
                p_update_assignment(booking.wsrbook_pidm , 'WR' , booking.wsrbook_term_code);
                lv_room_chg_ind := 'W';
            
              end if;
          
         -- else --Housing assignment changes not needed
        
          --dbms_output.put_line('Housing application not found. Cannot update assignment for ID: ' || lv_stud_id);  
          /*
          if (booking.wsrbook_ascd_code = 'INRM' or booking.wsrbook_ascd_code ='RESV') then
            --insert a new record with AR status
            p_insert_assignment(booking.wsrbook_pidm,booking.wsrbook_bldg_code, booking.wsrbook_room_number, booking.wsrbook_term_code,
                                  booking.wsrbook_begin_date,  booking.wsrbook_end_date ,booking.wsrbook_ascd_code);
          end if;                        
        */
        
    end if;
    close c_assignment;
  end if;
  close c_application;
  --Output with 2 spaces between fields

  dbms_output.put_line(rpad(booking.wsrbook_id,11,' ') || rpad(nvl(booking.wsrbook_term_code,' '),8,' ') || rpad(nvl(booking.wsrbook_bldg_code, ' '),6,' ')  || 
                       rpad(nvl(booking.wsrbook_room_number,' '),6,' ') || rpad(booking.wsrbook_ascd_code,6,' ')  || rpad(nvl(lv_assign_inst_ind, ' '),7,' ') 
                       || rpad(nvl(lv_room_chg_ind,' '),8,' ')
                       );
  --Reset output variables                       
  lv_room_chg_ind := null;                       
  lv_assign_inst_ind  := null;
end loop;


commit;

--EXCEPTION  -- exception handlers begin
 --  WHEN OTHERS THEN  
  --    dbms_output.put_line('Error processing booking for ID: ' || lv_stud_id);
end p_get_bookings;

-------------------------------------------------------
--Update an existinge housing assignment table
-------------------------------------------------------
 procedure p_update_assignment(pv_pidm spriden.spriden_pidm%type, pv_ascd_code stvascd.stvascd_code%type , pv_term_code stvterm.stvterm_code%type) is
 
 begin
 SAVEPOINT slrrasg_update;
 
 update slrrasg
                set    slrrasg_ascd_code = pv_ascd_code,
                       slrrasg_ascd_date = sysdate,
                       slrrasg_activity_date = sysdate,
                       slrrasg_data_origin = gv_data_origin,
                       slrrasg_user_id = gv_user_id
                where  slrrasg_pidm = pv_pidm
                and    slrrasg_term_code = pv_term_code
                and    slrrasg_ascd_code = 'AR';
                
  COMMIT;
  
  exception
    when others then
      dbms_output.put_line('The following Student has errors in load file.');
      ROLLBACK TO slrrasg_update;
 
 end p_update_assignment;
-------------------------------------------------------
--Insert a new record into the housing assignment table
-------------------------------------------------------
procedure p_insert_assignment(pv_pidm spriden.spriden_pidm%type, pv_bldg_code slbrdef.slbrdef_bldg_code%type, pv_room_number slbrdef.slbrdef_room_number%type, pv_term_code stvterm.stvterm_code%type,
                            pv_begin_date slrrasg.slrrasg_begin_date%type, pv_end_date slrrasg.slrrasg_end_date%type , pv_ascd_code slrrasg.slrrasg_ascd_code%type)  is
lv_check number;
begin

--Check if records exists
select count(*) into lv_check from slrrasg where slrrasg_pidm = pv_pidm and slrrasg_bldg_code = pv_bldg_code and slrrasg_term_code = pv_term_code and slrrasg_begin_date = pv_begin_date and slrrasg_end_date = pv_end_date
and slrrasg_ascd_code =  decode(pv_ascd_code,'INRM','AR','RESV','RE','CNCL','WR');

SAVEPOINT slrrasg_insert;

if lv_check = 0 then 
 insert into slrrasg(slrrasg_pidm, slrrasg_bldg_code, slrrasg_room_number, slrrasg_term_code, slrrasg_rrcd_code, slrrasg_begin_date, slrrasg_end_date,
                          slrrasg_total_days, slrrasg_total_months, slrrasg_total_terms, slrrasg_ascd_code, slrrasg_ascd_date, slrrasg_onl_or_bat,
                          slrrasg_activity_date, slrrasg_ar_ind, slrrasg_overload_ind, slrrasg_roll_ind, slrrasg_override_error,
                          slrrasg_assess_needed, slrrasg_data_origin, slrrasg_user_id)
                  values (pv_pidm,
                          pv_bldg_code,
                          pv_room_number,
                          pv_term_code,
                          'SA',
                          /*
                          (select a.slbrdef_rrcd_code
                           from   slbrdef a
                           where  a.slbrdef_bldg_code = pv_bldg_code
                           and    a.slbrdef_room_number = pv_room_number
                           and    a.slbrdef_term_code_eff = (select max(b.slbrdef_term_code_eff)
                                                             from   slbrdef b
                                                             where  b.slbrdef_bldg_code = a.slbrdef_bldg_code
                                                             and    b.slbrdef_room_number = a.slbrdef_room_number
                                                             and    b.slbrdef_term_code_eff <= pv_term_code)),
                            */                          
                          pv_begin_date,
                          pv_end_date,
                          0,
                          0,
                          1,
                         decode(pv_ascd_code,'INRM','AR','RESV','RE','CNCL','WR'),
                          SYSDATE,
                          'O',
                          SYSDATE,
                          gv_ar_ind, --decode(pv_ascd_code, 'RESV','N','Y'), --Do not assess fees for Reservations
                          NULL,
                          NULL,
                          NULL,
                          gv_assess_charges,
                          gv_data_origin,
                          gv_user_id);
                              
end if;

commit;

exception
  when others then
  dbms_output.put_line('The following Student has errors in load file.');
  ROLLBACK TO slrrasg_insert;
      
end p_insert_assignment;

----------------------------------------------------
-- Process Meal applications and assignments
----------------------------------------------------
procedure p_get_meals is
  --FTT Import 
  cursor c_meals is
    select wsrmeal_pidm, wsrmeal_id, wsrmeal_term_code, wsrmeal_change_date , wsrmeal_plan_status , wsrmeal_plan_code,wsrmeal_begin_date, wsrmeal_end_date , wsrmeal_plan_type 
          from darton.wsrmeal;
    
     lv_meal_requests c_meals%rowtype;
        lv_application_type stvartp.stvartp_code%type;
        lv_meal_app number;
        lv_meal_assign number;
        lv_assignment_dml varchar2(1);
         lv_application_dml varchar2(1);
begin
    
    dbms_output.put_line('----------- Processing Meal Plan Requests -----------');
      dbms_output.put_line('Legend: (N)ew, (U)pdate');
      dbms_output.put_line(rpad('STUD ID',11,' ') || rpad('TERM',8,' ') || rpad('STATUS',8,' ') || rpad('CODE',6,' ') || rpad('TYPE',6,' ') || rpad('APPL',6,' ')   || rpad('ASSGN',7,' ') 
                          );
   open c_meals;
    loop
      fetch c_meals into lv_meal_requests;
      exit when c_meals%NOTFOUND;
       
       case lv_meal_requests.wsrmeal_plan_type
       when 'R' then lv_application_type := 'HOME';
       when 'C' then lv_application_type := 'MEAL';
       else  lv_application_type := 'MEAL';
       end case;
      
        --Either create a new application or update an existing one if available
        select count(*)
        into lv_meal_app
        from   slbrmap
        where  slbrmap_pidm = lv_meal_requests.wsrmeal_pidm
        and    slbrmap_from_term = lv_meal_requests.wsrmeal_term_code;
        
        if lv_meal_app = 0 then
          --create a new application
          insert into slbrmap (slbrmap_pidm, slbrmap_mrcd_code, 
                                                slbrmap_haps_code,  slbrmap_artp_code,
                                                slbrmap_from_term,  slbrmap_to_term,
                                                slbrmap_activity_date, slbrmap_user_id , slbrmap_data_origin) 
                                                values 
                                                (lv_meal_requests.wsrmeal_pidm , lv_meal_requests.wsrmeal_plan_code,
                                                lv_meal_requests.wsrmeal_plan_status,  lv_application_type,
                                                lv_meal_requests.wsrmeal_term_code,lv_meal_requests.wsrmeal_term_code,
                                                sysdate,  gv_user_id , gv_data_origin );

          lv_application_dml := 'N';
        else
         --update an existing application       
         update slbrmap set slbrmap_mrcd_code = lv_meal_requests.wsrmeal_plan_code ,   slbrmap_haps_code=lv_meal_requests.wsrmeal_plan_status , slbrmap_artp_code=lv_application_type,
          slbrmap_activity_date = sysdate, slbrmap_user_id=gv_user_id , slbrmap_data_origin=gv_data_origin
              where slbrmap_pidm = lv_meal_requests.wsrmeal_pidm and slbrmap_from_term = lv_meal_requests.wsrmeal_term_code ;
              lv_application_dml := 'U';
        end if;
        
            select count(*)
            into lv_meal_assign
        from   slrmasg
        where  1=1
        and    slrmasg_pidm = lv_meal_requests.wsrmeal_pidm
        and    slrmasg_term_code = lv_meal_requests.wsrmeal_term_code;
        
        if (lv_meal_assign = 0 ) then 
        --create a new meal assignment
          insert into slrmasg (slrmasg_pidm , slrmasg_term_code , slrmasg_mrcd_code , 
                               slrmasg_begin_date , slrmasg_end_date , slrmasg_total_days , 
                               slrmasg_total_months , slrmasg_total_terms , 
                               slrmasg_mscd_code , slrmasg_mscd_date , slrmasg_onl_or_bat , SLRMASG_ASSESS_NEEDED,
                               slrmasg_activity_Date , slrmasg_ar_ind , slrmasg_user_id , slrmasg_data_origin)
                 values
                            (lv_meal_requests.wsrmeal_pidm , lv_meal_requests.wsrmeal_term_code , lv_meal_requests.wsrmeal_plan_code,
                            lv_meal_requests.wsrmeal_begin_date, lv_meal_requests.wsrmeal_end_date , 0,
                            0,1,
                            lv_meal_requests.wsrmeal_plan_status , lv_meal_requests.wsrmeal_change_date , 'B', gv_assess_charges,
                            sysdate , gv_ar_ind , gv_user_id , gv_data_origin);
            lv_assignment_dml := 'N';
      
      else  
     
         update slrmasg set slrmasg_mrcd_code = lv_meal_requests.wsrmeal_plan_code ,  
         slrmasg_mscd_code = lv_meal_requests.wsrmeal_plan_status , slrmasg_activity_date = sysdate, slrmasg_user_id = gv_user_id, slrmasg_data_origin = gv_data_origin
         where slrmasg_pidm = lv_meal_requests.wsrmeal_pidm and slrmasg_term_code = lv_meal_requests.wsrmeal_term_code;
          lv_assignment_dml := 'U';
         end if; 
       
        dbms_output.put_line(rpad(lv_meal_requests.wsrmeal_id,11,' ') || rpad(lv_meal_requests.wsrmeal_term_code,8,' ')  
                            || rpad(lv_meal_requests.wsrmeal_plan_status,8,' ') || rpad(lv_meal_requests.wsrmeal_plan_code,6,' ') || rpad(lv_application_type,6,' ')
                            || rpad(lv_application_dml,6,' ') || rpad(lv_assignment_dml,7, ' ')
                            );
       lv_application_dml := null;
       lv_assignment_dml := null;
       lv_application_type := null;
      end loop;
      
      
  close c_meals;
  
     
end p_get_meals;

END wstarrez;
/
