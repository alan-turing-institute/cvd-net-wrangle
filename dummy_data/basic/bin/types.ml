

type sex = Male | Female | Missing
let count_sexes = List.length [Male;Female]
let sex_of_index (i:int) : sex = 
match i with
  | 0 -> Male
  | 1 -> Female
  | _ -> Missing

let string_of_sex (s:sex):string=
match s with
| Male -> "Male"
| Female -> "Female"
| Missing -> "Missing"

type patient = {
  id: int;
  sex: sex;
  date_of_birth: Unix.tm;
  referral_at_day: int;
  diagnosis_at_day: int;
  treatment_at_day: int;
  death_at_day: int option;
  (* weight: float; *)
}

type statistics = {
  referral_at_days: int;
  diagnosis_at_days_after_referral: int;
  start_treatment_at_days_after_diagnosis: int;
}

