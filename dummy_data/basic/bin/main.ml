open Types
open Utilities

(* This module generates dummy patient data for testing purposes. *)

(* The patient record type *)

(* let current_date = Unix.gmtime (Unix.time ()) *)

(* let print_patient (p:patient): unit =
    (* let age = years_delta p.date_of_birth current_date in *)
    (* let age_at_referral = years_delta p.date_of_birth p.date_of_referral in *)
    Printf.printf "ID:%i \tsex:%s \tdob:%s \referral_at_day:%i diagnosis_at_day:%i \n" 
        p.id (string_of_sex p.sex) (print_date p.date_of_birth) p.referral_at_day p.diagnosis_at_day; *)


(* let generate_patients (n:int) : patient list =
    let oldest_dob = generate_date ~years_since_1900:60 in
    let youngest_dob = generate_date ~years_since_1900:100 in
    List.init n (fun i -> 
        let sex = sex_of_index (random_int ~min:0 ~max:count_sexes) in
        let date_of_birth = random_date ~start:oldest_dob ~end_:youngest_dob in
        let age_in_days = (years_delta date_of_birth current_date) * 365 in
        let referral_at_day = gaussian ~mu:age ~sigma:(age /. 2.) *. 365. in
        let date_of_referral = next_date date_of_birth (int_of_float @@ sample_age_referral ()) in
        let sample_time_to_diagnosis = gaussian ~mu:7. ~sigma:10. in
        generate_patient (i+1) date_of_birth sex date_of_referral 

    ) *)


(* Main function *)
let () =
    Random.init 3;
    let cwd = Sys.getcwd () in
    Printf.printf "Current working directory: %s\n" cwd;
    let path = Fpath.v  "bin/config.yaml" in
    let yaml = load_yaml path in
    let stats = statistics_of_yaml yaml in
    Printf.printf "Referral at days: %i\n" stats.referral_at_days;