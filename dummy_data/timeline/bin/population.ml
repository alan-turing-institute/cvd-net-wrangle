open Types
open Utilities
(* open Events
open Observations *)

let print_patient (p:patient): unit =
  let current_date = Unix.gmtime (Unix.time ()) in
  let age = years_delta p.date_of_birth current_date in
  let events:event list = p.events in
  let observations: observation list = p.observations in
  let output = Printf.sprintf "\n\nID:%i \tsex:%s \tage:%i" p.id (string_of_sex p.sex) age in
  let output = output ^ String.concat "" (List.map (fun e -> Printf.sprintf "\n Event: %s at day after referral %i" e.ev_name e.recorded_on_day_after_referral) events) in
  let output = output ^ String.concat "" (List.map (fun o -> Printf.sprintf "\n Observation %s: %s at day %i" o.obs_name o.value_as_string o.recorded_on_day_after_referral) observations) in
  Printf.printf "\n %s" output

let generate_population (num_patients:int)(statistics: (string, int) Hashtbl.t) : patient list =
  let oldest_dob = generate_date ~years_since_1900:(Hashtbl.find statistics "earliest_dob_year")  in
  let youngest_dob = generate_date ~years_since_1900:(Hashtbl.find statistics "latest_dob_year") in
  List.init num_patients (fun i -> 
      let sex = sex_of_index (random_int ~min:0 ~max:count_sexes) in
      let date_of_birth = random_date ~start:oldest_dob ~end_:youngest_dob in
      let patient = {
        id = i;
        sex = sex;
        date_of_birth = date_of_birth;
        events = [];
        observations = []
      } in
      let patient = { patient with events = Events.generate_events statistics} in
      let patient = 
        match sex with
        | Male -> { patient with observations = Observations.generate_observations statistics Observations.male_observation_keys}
        | Female -> { patient with observations = Observations.generate_observations statistics Observations.female_observation_keys}
        | _ -> { patient with observations = [] } in patient
  )