open Types
open Utilities

let male_observation_keys : statistics_key list = [
  {label="weight"; mean="mean_weight_male"; std="std_weight_male"};
  ]

let female_observation_keys : statistics_key list = [
  {label="weight_female"; mean="mean_weight_female"; std="std_weight_female"};
  ]

let generate_observations (statistics:(string, int)Hashtbl.t) (observation_keys:statistics_key list) : observation list =
  List.map (fun k -> 
      let mean = float_of_int (Hashtbl.find statistics k.mean) in
      let std = float_of_int (Hashtbl.find statistics k.std) in
      {
        recorded_on_day_after_referral = 0;
        name = k.label;
        value_as_string = string_of_float (gaussian ~mu:mean ~sigma:std)
      }
    ) observation_keys