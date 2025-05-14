open Types
open Utilities

let event_keys : statistics_key list = [
  {label="referral day after dob"; mean="mean_referral_at_day"; std="std_referral"};
  {label="diagnosis day after referral"; mean="mean_diagnosis_at_day_after_referral"; std="std_diagnosis"};
  {label="treatment start day after diagnosis"; mean="mean_treatment_at_day_after_diagnosis"; std="std_treatment"}
  ]

let generate_events (statistics:(string, int)Hashtbl.t) : event list =
  List.map (fun k -> 
      let mean = float_of_int (Hashtbl.find statistics k.mean) in
      let std = float_of_int (Hashtbl.find statistics k.std) in
      let value = int_of_float (gaussian ~mu:mean ~sigma:std) in
      {
        name = k.label;
        day = value;
      }
    ) event_keys

(* let populate_with_events (patients: patient list)(statistics: (string, int) Hashtbl.t) :patient list =
  List.map (fun p -> 
    let evts = generate_events statistics in
    { p with events = evts}) patients
     *)
    

  (* let mean_referral = float_of_int (Hashtbl.find statistics "mean_referral_at_day") in
  let std_referral = 14. in
  let mean_diagnosis = float_of_int (Hashtbl.find statistics "mean_diagnosis_at_day_after_referral") in
  let std_diagnosis = 14. in
  let mean_treatment = float_of_int (Hashtbl.find statistics "mean_treatment_at_day_after_diagnosis") in
  let std_treatment = 14. in
  let referral_at_day = int_of_float (gaussian ~mu:mean_referral ~sigma:std_referral) in
  [{
      day = referral_at_day;
      name = "referral_at_day";
  }] *)