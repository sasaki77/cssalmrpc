SQL_CURRENT_ALARM = """
                    SELECT
                      pv.alarm_time,
                      CASE
                        WHEN at4.name = '{0}'
                          THEN at3.name
                        WHEN at3.name = '{0}'
                          THEN at2.name
                        ELSE at4.name
                      END AS group,
                      CASE
                        WHEN at4.name = '{0}'
                          THEN at2.name
                        WHEN at3.name = '{0}'
                          THEN NULL
                        ELSE at3.name
                      END AS sub_group,
                      CASE
                        WHEN at4.name != '{0}'
                             AND at3.name != '{0}'
                          THEN at2.name
                        ELSE NULL
                      END as sub_sub_group,
                      severity.name,
                      status.name,
                      pv.descr,
                      at1.name as pv_name,
                      pv.severity_id
                    FROM
                      pv
                        JOIN status
                          ON status.status_id = pv.status_id
                        JOIN severity
                          ON severity.severity_id = pv.severity_id
                        JOIN alarm_tree AS at1
                          ON at1.component_id = pv.component_id
                        LEFT JOIN alarm_tree AS at2
                          ON at1.parent_cmpnt_id = at2.component_id
                        LEFT JOIN alarm_tree AS at3
                          ON at2.parent_cmpnt_id = at3.component_id
                        LEFT JOIN alarm_tree AS at4
                          ON at3.parent_cmpnt_id = at4.component_id
                    WHERE
                      pv.severity_id > 1
                      AND (at3.name = '{0}' OR at4.name = '{0}') 
                    """
