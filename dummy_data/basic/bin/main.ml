open Types
open Utilities

(* This module generates dummy patient data for testing purposes. *)

(* The patient record type *)

let current_date = Unix.gmtime (Unix.time ())
let generate_patient (id:int) (date_of_birth:Unix.tm)(sex:sex) (date_of_referral:Unix.tm) : patient = 
    {id=id; date_of_birth=date_of_birth; sex=sex; date_of_referral=date_of_referral}

let print_patient (p:patient): unit =
    let age = years_delta p.date_of_birth current_date in
    let age_at_referral = years_delta p.date_of_birth p.date_of_referral in
    Printf.printf "ID:%i \tsex:%s \tdob:%s \tdate_of_referral:%s age: %i age_at_referral: %i\n" 
        p.id (string_of_sex p.sex) (print_date p.date_of_birth) (print_date p.date_of_referral) age age_at_referral

let generate_date ~(years_since_1900:int) : Unix.tm = 
    {
        Unix.tm_sec = 0;
        tm_min = 0;
        tm_hour = 0;
        tm_mday = 1;
        tm_mon = 0;    (* January = 0 *)
        tm_year = years_since_1900; (* 2000 - 1900 = 100 *)
        tm_wday = 0; tm_yday = 0; tm_isdst = false
    } 

let generate_patients (n:int) : patient list =
    let oldest_dob = generate_date ~years_since_1900:60 in
    let youngest_dob = generate_date ~years_since_1900:100 in
    List.init n (fun i -> 
        let sex = sex_of_index (random_int ~min:0 ~max:count_sexes) in
        let date_of_birth = random_date ~start:oldest_dob ~end_:youngest_dob in
        let age = float_of_int @@ years_delta date_of_birth current_date in
        let sample_age_referral = gaussian ~mu:age ~sigma:(age /. 2.) in
        let date_of_referral = next_date date_of_birth (int_of_float @@ sample_age_referral ()) in
        generate_patient (i+1) date_of_birth sex date_of_referral
    )

let () =
    Random.init 3;
    let patients = generate_patients 10 in
    List.iter (fun p -> print_patient p) @@ patients;

    


