CREATE TABLE IF NOT EXISTS searchExtrCrewWrkPstt (
    username TEXT NOT NULL,
    pjtDt TEXT NOT NULL,
    pdiaNo TEXT NOT NULL,
    repTrn1No TEXT,
    gwkTm TEXT,
    repTrn2No TEXT,
    loiwTm TEXT,
    empNm1 TEXT,
    empNm2 TEXT,
    empNm3 TEXT,
    empNm4 TEXT,
    createdAt TEXT DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updatedAt TEXT DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (username, pjtDt, pdiaNo)
);

CREATE INDEX IF NOT EXISTS idx_search_extr_crew_wrk_pstt_username ON searchExtrCrewWrkPstt (username);
CREATE INDEX IF NOT EXISTS idx_search_extr_crew_wrk_pstt_pjtdt ON searchExtrCrewWrkPstt (pjtDt);
CREATE INDEX IF NOT EXISTS idx_search_extr_crew_wrk_pstt_composite ON searchExtrCrewWrkPstt (username, pjtDt);
