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

SQL_PV_LIST = """
              SELECT
                at1.name AS name,
                pv.descr,
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
                END as sub_sub_group
              FROM
                pv
                JOIN alarm_tree AS at1
                  ON at1.component_id = pv.component_id
                LEFT JOIN alarm_tree AS at2
                  ON at1.parent_cmpnt_id = at2.component_id
                LEFT JOIN alarm_tree AS at3
                  ON at2.parent_cmpnt_id = at3.component_id
                LEFT JOIN alarm_tree AS at4
                  ON at3.parent_cmpnt_id = at4.component_id
              """

SQL_HISTORY_ALL = """
                  SELECT
                    message.id AS id,
                    message.datum AS datum,
                    message.name AS record_name,
                    message.severity AS severity,
                    MAX(
                      CASE
                        WHEN message_content.msg_property_type_id = 2
                          THEN message_content.value
                        ELSE '-'
                      END
                    ) AS eventtime,
                    MAX(
                      CASE
                        WHEN message_content.msg_property_type_id = 19
                          THEN message_content.value
                        ELSE '-'
                      END
                    ) AS status
                  FROM
                    message
                    JOIN message_content
                      ON message.id=message_content.message_id
                         AND (message_content.msg_property_type_id = 19
                              OR message_content.msg_property_type_id = 2
                              )
                  WHERE
                    message.datum BETWEEN %s AND %s
                    AND message.type='alarm'
                  GROUP BY
                    message.id
                  ORDER BY
                    message.id, message.datum
                  LIMIT
                    100000
                  """

SQL_HISTORY_GROUP = """
                    SELECT
                      message.id AS id,
                      message.datum AS datum,
                      message.name AS record_name,
                      message.severity AS severity,
                      MAX(
                        CASE
                          WHEN message_content.msg_property_type_id = 2
                            THEN message_content.value
                          ELSE '-'
                        END
                      ) AS eventtime,
                      MAX(
                        CASE
                          WHEN message_content.msg_property_type_id = 19
                            THEN message_content.value
                          ELSE '-'
                        END
                      ) AS status
                    FROM
                      message
                      JOIN message_content
                        ON message.id=message_content.message_id
                           AND (message_content.msg_property_type_id = 19
                                OR message_content.msg_property_type_id = 2
                                )
                    WHERE
                      message.datum BETWEEN %s AND %s
                      AND message.type='alarm'
                      AND message.name=ANY(%s)
                    GROUP BY
                      message.id
                    ORDER BY
                      message.id, message.datum
                    LIMIT
                      100000
                    """
