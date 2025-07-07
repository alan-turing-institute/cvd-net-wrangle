

CREATE TABLE participant (
  id VARCHAR(10) NOT NULL,
  diagnosis VARCHAR(100),
  dob TIMESTAMP,
  height_m NUMERIC(5,2),
  sex_id INTEGER,
  PRIMARY KEY (id),
  FOREIGN KEY (sex_id) REFERENCES sex(id)
);

CREATE TABLE visit (
  id SERIAL NOT NULL,
  participant_id VARCHAR(10) NOT NULL,
  study_week INTEGER NOT NULL,
  days_after_anchor INTEGER,
  PRIMARY KEY (id),
  FOREIGN KEY (participant_id) REFERENCES participant(id)
);

CREATE TABLE visit_label (
  id SERIAL NOT NULL,
  label VARCHAR(10) NOT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE comorbidity (
  id SERIAL NOT NULL,
  visit_id INTEGER NOT NULL,
  comorbidity VARCHAR(100) NOT NULL,
  PRIMARY KEY (id),
  FOREIGN KEY (visit_id) REFERENCES visit(id)
);

CREATE TABLE weight (
  id SERIAL NOT NULL,
  visit_id INTEGER NOT NULL,
  weight_kg NUMERIC(5,2) NOT NULL,
  PRIMARY KEY (id),
  FOREIGN KEY (visit_id) REFERENCES visit(id)
);

CREATE TABLE ntp_pro_bnp (
  id SERIAL NOT NULL,
  visit_id INTEGER NOT NULL,
  NTProBNP_pgML NUMERIC(5,2) NOT NULL,
  PRIMARY KEY (id),
  FOREIGN KEY (visit_id) REFERENCES visit(id)
);

CREATE TABLE 6_min_walk_distance (
  id SERIAL NOT NULL,
  6mwd_m INTEGER,
  visit_id INTEGER NOT NULL,
  notes VARCHAR(100),
  PRIMARY KEY (id),
  FOREIGN KEY (visit_id) REFERENCES visit(id)
);

CREATE TABLE gad2 (
  id SERIAL NOT NULL,
  score INTEGER,
  visit_id INTEGER NOT NULL,
  PRIMARY KEY (id),
  FOREIGN KEY (visit_id) REFERENCES visit(id)
);

CREATE TABLE gad7 (
  id SERIAL NOT NULL,
  score INTEGER,
  visit_id INTEGER NOT NULL,
  PRIMARY KEY (id),
  FOREIGN KEY (visit_id) REFERENCES visit(id)
);

CREATE TABLE phq2 (
  id SERIAL NOT NULL,
  score INTEGER,
  visit_id INTEGER NOT NULL,
  PRIMARY KEY (id),
  FOREIGN KEY (visit_id) REFERENCES visit(id)
);

CREATE TABLE phq7 (
  id SERIAL NOT NULL,
  score INTEGER,
  visit_id INTEGER NOT NULL,
  PRIMARY KEY (id),
  FOREIGN KEY (visit_id) REFERENCES visit(id)
);

INSERT INTO sex (category) VALUES ('Female'),('Male');