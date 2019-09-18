using Oracle.ManagedDataAccess.Client;
using Rebus.Config;
using Rebus.Exceptions;
using Rebus.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace Rebus.Oracle.Transport
{
    public class OracleAQInitializationOptions
    {
        public OracleAQInitializationOptions()
        {
        }

        public string TableName { get; set; }
        public string InputQueueName { get; set; }
    }

    public class OracleAQSchemaInitializer
    {
        private readonly OracleConnectionHelper _connectionHelper;
        private readonly ILog _log;
        public OracleAQSchemaInitializer(IRebusLoggerFactory loggerFactory, OracleConnectionHelper connectionHelper)
        {
            _log = loggerFactory.GetLogger<OracleAQSchemaInitializer>();
            _connectionHelper = connectionHelper;
        }

        /// <summary>
        /// Creates the necessary table
        /// </summary>
        public void EnsureQueueIsCreated(OracleAQInitializationOptions options)
        {
            try
            {
                CreateSchema(options);
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, $"Error attempting to initialize SQL transport schema with mesages table [dbo].[{options.TableName}]");
            }
        }

        void CreateSchema(OracleAQInitializationOptions options)
        {
            using (var connection = _connectionHelper.GetConnection())
            {
                var tableNames = connection.GetTableNames();

                if (tableNames.Contains(options.TableName, StringComparer.OrdinalIgnoreCase))
                {
                    _log.Info("Database already contains a table named {tableName} - will not create anything", options.TableName);
                    return;
                }

                _log.Info("Table {tableName} does not exist - it will be created with the queue now", options.TableName);

                ExecuteCommands(connection, $@"
                    DECLARE
                        v_count NUMBER(10);
                    BEGIN
                        SELECT COUNT(*) INTO v_count
                        FROM user_objects
                        WHERE 
                             object_type = 'TYPE'
                         AND UPPER(object_name) = 'REBUS_MESSAGE_V2';

                        IF v_count = 0 THEN
                            EXECUTE IMMEDIATE 'CREATE OR REPLACE TYPE REBUS_MSG_HEADER AS OBJECT (
                                           KEY VARCHAR2(4000),
                                           VALUE VARCHAR2(4000)
                                        );';

                            EXECUTE IMMEDIATE 'CREATE OR REPLACE TYPE REBUS_MSG_HEADERS IS VARRAY(100) OF REBUS_MSG_HEADER NOT NULL;';


                            EXECUTE IMMEDIATE 'create or replace type REBUS_MESSAGE_V2 as object
                                                (
                                                      HEADERS         REBUS_MSG_HEADERS,                                           
                                                      BODY_RAW        RAW(2000),
                                                      BODY_BLOB       BLOB,
                                                      MEMBER FUNCTION ContainsHeader(p_header_key VARCHAR2) RETURN NUMBER,
                                                      MEMBER FUNCTION GetHeaderValue(p_header_key VARCHAR2)RETURN VARCHAR2
                                                );
                                                /
                                                create or replace type body REBUS_MESSAGE_V2 is
  
                                                  -- Member procedures and functions
                                                  MEMBER FUNCTION ContainsHeader(p_header_key VARCHAR2) RETURN NUMBER
                                                  IS
                                                  BEGIN
                                                    FOR i IN 1..HEADERS.Count LOOP
                                                      BEGIN
                                                        IF HEADERS(i).KEY = p_header_key
                                                        THEN
                                                          RETURN 1;
                                                        END IF;
                                                      END;
                                                    END LOOP;
         
                                                    RETURN 0;
                                                  END; 
  
                                                  MEMBER FUNCTION GetHeaderValue(p_header_key VARCHAR2)RETURN VARCHAR2
                                                  IS        
                                                  BEGIN
    
                                                    FOR i IN 1..HEADERS.Count LOOP
                                                      BEGIN
                                                        IF HEADERS(i).KEY = p_header_key
                                                        THEN
                                                          RETURN HEADERS(i).VALUE;
                                                        END IF;
                                                      END;
                                                    END LOOP;
        
                                                    RETURN NULL;
                                                  END; 
  
                                                end;
                                                /';

                            EXECUTE IMMEDIATE 'create or replace package PKG_REBUS is
                                                  TYPE VARCHAR2_ARRAY IS TABLE OF VARCHAR2(4000)  -- Associative array type
                                                       INDEX BY PLS_INTEGER;      

                                                  PROCEDURE P_Enqueue_Rebus_Msg
                                                  (
                                                       p_Queue_Name         IN    VARCHAR2,
                                                       p_EO_Visibility      IN    BINARY_INTEGER,       
                                                       p_EO_Transformation  IN    VARCHAR2,
                                                       p_EO_Delivery_Mode   IN    PLS_INTEGER,
                                                       p_MP_Priority        IN    BINARY_INTEGER,
                                                       p_MP_Delay           IN    BINARY_INTEGER,
                                                       p_MP_Expiration      IN    BINARY_INTEGER,
                                                       p_Headers_Keys       IN    VARCHAR2_ARRAY,
                                                       p_Headers_Values     IN    VARCHAR2_ARRAY,
                                                       p_Body_Raw           IN    RAW,
                                                       p_Body_Blob          IN    BLOB
                                                  );
  
                                                  PROCEDURE P_Dequeue_Rebus_Msg
                                                  (
                                                       p_Queue_Name         IN    VARCHAR2,
                                                       p_DO_Consumers_Names IN    VARCHAR2_ARRAY,
                                                       p_DO_Visibility      IN    BINARY_INTEGER,
                                                       p_DO_Wait            IN    BINARY_INTEGER, 
                                                       p_DO_Transformation  IN    VARCHAR2,
                                                       p_DO_Delivery_Mode   IN    PLS_INTEGER,       
                                                       p_Headers_Keys       OUT   VARCHAR2_ARRAY,
                                                       p_Headers_Values     OUT   VARCHAR2_ARRAY,
                                                       p_Body_Raw           OUT   RAW,
                                                       p_Body_Blob          OUT   BLOB
                                                   );

                                                end PKG_REBUS;
                                                /
                                                create or replace package body PKG_REBUS is

                                                   PROCEDURE P_Enqueue_Rebus_Msg
                                                   (
                                                       p_Queue_Name         IN    VARCHAR2,
                                                       p_EO_Visibility      IN    BINARY_INTEGER,       
                                                       p_EO_Transformation  IN    VARCHAR2,
                                                       p_EO_Delivery_Mode   IN    PLS_INTEGER,
                                                       p_MP_Priority        IN    BINARY_INTEGER,
                                                       p_MP_Delay           IN    BINARY_INTEGER,
                                                       p_MP_Expiration      IN    BINARY_INTEGER,
                                                       p_Headers_Keys       IN    VARCHAR2_ARRAY,
                                                       p_Headers_Values     IN    VARCHAR2_ARRAY,
                                                       p_Body_Raw           IN    RAW,
                                                       p_Body_Blob          IN    BLOB
                                                   ) IS
                                                      v_Enqueue_Options     dbms_aq.enqueue_options_t;
                                                      v_Message_Properties  dbms_aq.message_properties_t;
                                                      v_message_handle      RAW(16);
                                                      v_headers             rebus_msg_headers;
                                                   BEGIN
                                                      v_Enqueue_Options.visibility      := p_EO_Visibility;
                                                      v_Enqueue_Options.delivery_mode   := p_EO_Delivery_Mode;
                                                      v_Enqueue_Options.transformation  := p_EO_Transformation;

                                                      v_Message_Properties.priority     := p_MP_Priority;
                                                      v_Message_Properties.delay        := p_MP_Delay;
                                                      v_Message_Properties.expiration   := p_MP_Expiration;
        
                                                      v_headers                         := rebus_msg_headers();
                                                      v_headers.extend(p_Headers_Keys.count);

                                                      FOR i IN 1..p_Headers_Keys.count LOOP
                                                        v_headers(i) := rebus_msg_header(p_Headers_Keys(i), p_Headers_Values(i));
                                                      END LOOP;                    

                                                      DBMS_AQ.ENQUEUE(
                                                          QUEUE_NAME => p_Queue_Name,
                                                          ENQUEUE_OPTIONS => v_Enqueue_Options,
                                                          MESSAGE_PROPERTIES => v_Message_Properties,
                                                          PAYLOAD => new REBUS_MESSAGE_V2(v_headers, p_Body_Raw, p_Body_Blob),
                                                          MSGID => v_message_handle);
                                                   END P_Enqueue_Rebus_Msg;
   
                                                   PROCEDURE P_Dequeue_Rebus_Msg
                                                   (
                                                       p_Queue_Name         IN    VARCHAR2,
                                                       p_DO_Consumers_Names IN    VARCHAR2_ARRAY,
                                                       p_DO_Visibility      IN    BINARY_INTEGER,
                                                       p_DO_Wait            IN    BINARY_INTEGER, 
                                                       p_DO_Transformation  IN    VARCHAR2,
                                                       p_DO_Delivery_Mode   IN    PLS_INTEGER,       
                                                       p_Headers_Keys       OUT   VARCHAR2_ARRAY,
                                                       p_Headers_Values     OUT   VARCHAR2_ARRAY,
                                                       p_Body_Raw           OUT   RAW,
                                                       p_Body_Blob          OUT   BLOB
                                                   ) IS
                                                      v_Dequeue_Options     dbms_aq.DEQUEUE_OPTIONS_T;
                                                      v_Message_Properties  dbms_aq.message_properties_t;
                                                      v_Message_Handle      RAW(16);
                                                      v_Payload             REBUS_MESSAGE_V2;
                                                      v_Agent_List          DBMS_AQ.AQ$_AGENT_LIST_T;
                                                      v_Agent               SYS.AQ$_AGENT;
                                                   BEGIN
                                                      v_Dequeue_Options.visibility      := p_DO_Visibility;
                                                      v_Dequeue_Options.delivery_mode   := p_DO_Delivery_Mode;
                                                      v_Dequeue_Options.transformation  := p_DO_Transformation;
                                                      v_Dequeue_Options.WAIT            := p_DO_Wait;
    
                                                      IF p_DO_Consumers_Names IS NULL OR 
                                                         p_DO_Consumers_Names.count = 0 OR 
                                                         p_DO_Consumers_Names(1) IS NULL 
                                                      THEN
                                                        DBMS_AQ.DEQUEUE(
                                                            QUEUE_NAME => p_Queue_Name,
                                                            DEQUEUE_OPTIONS => v_Dequeue_Options,
                                                            MESSAGE_PROPERTIES => v_Message_Properties,
                                                            PAYLOAD => v_Payload,
                                                            MSGID => v_Message_Handle);
    
                                                        FOR i IN 1..v_Payload.HEADERS.count LOOP
                                                          p_Headers_Keys(i) := v_Payload.HEADERS(i).KEY;
                                                          p_Headers_Values(i) := v_Payload.HEADERS(i).VALUE;
                                                        END LOOP;  
                                                
                                                        p_Body_Raw       := v_Payload.BODY_RAW;
                                                        p_Body_Blob      := v_Payload.BODY_BLOB;
                                                      ELSE      
                                                        FOR i IN 1..p_DO_Consumers_Names.count LOOP
                                                          v_Agent_List(i) := SYS.AQ$_AGENT(p_DO_Consumers_Names(i), p_Queue_Name, NULL);
                                                        END LOOP;         

                                                        DBMS_AQ.LISTEN (
                                                            agent_list => v_Agent_List,
                                                            wait => p_DO_Wait,
                                                            agent => v_Agent);
      
                                                        IF v_Agent IS NOT NULL 
                                                        THEN
                                                          v_Dequeue_Options.CONSUMER_NAME   := v_Agent.name;
                            
                                                          DBMS_AQ.DEQUEUE(
                                                              QUEUE_NAME => p_Queue_Name,
                                                              DEQUEUE_OPTIONS => v_Dequeue_Options,
                                                              MESSAGE_PROPERTIES => v_Message_Properties,
                                                              PAYLOAD => v_Payload,
                                                              MSGID => v_Message_Handle);
        
                                                          FOR i IN 1..v_Payload.HEADERS.count LOOP
                                                            p_Headers_Keys(i) := v_Payload.HEADERS(i).KEY;
                                                            p_Headers_Values(i) := v_Payload.HEADERS(i).VALUE;
                                                          END LOOP;  
        
                                                          p_Headers_Keys(v_Payload.HEADERS.count + 1) := 'rbs2-consumer-name';
                                                          p_Headers_Values(v_Payload.HEADERS.count + 1) := v_Agent.name;
                                                                
                                                          p_Body_Raw       := v_Payload.BODY_RAW;
                                                          p_Body_Blob      := v_Payload.BODY_BLOB;
                                                        END IF;     
                                                      END IF;
                                                   EXCEPTION 
                                                     WHEN OTHERS THEN
                                                      IF SQLCODE = -25228 --ORA-25228 Timeout or End-of-Fetch Error When Dequeuing Messages
                                                         OR SQLCODE = -25254 --ORA-25254: time-out in LISTEN while waiting for a message
                                                      THEN      
                                                        p_Body_Raw         := NULL;
                                                        p_Body_Blob        := NULL;
                                                      ELSE
                                                         RAISE;
                                                      END IF;    
                                                   END P_Dequeue_Rebus_Msg;
  
                                                end PKG_REBUS;
                                                /';
                        END IF;
                    END;
                    ----
                    BEGIN
                    -- Call the procedure
                        sys.dbms_aqadm.create_queue_table(queue_table => '{options.TableName}',
                                                        queue_payload_type => 'REBUS_MESSAGE_V2', 
                                                        primary_instance => 1);
                                    
                        sys.dbms_aqadm.create_queue(queue_name => '{options.InputQueueName}',
                                                    queue_table => '{options.TableName}',
                                                    max_retries => 10, 
                                                    retry_delay => 3);

                        sys.dbms_aqadm.start_queue(queue_name => '{options.InputQueueName}');
                    END;
                ");

                connection.Complete();
            }
        }

        static void ExecuteCommands(OracleDbConnection connection, string sqlCommands)
        {
            foreach (var sqlCommand in sqlCommands.Split(new[] { "----" }, StringSplitOptions.RemoveEmptyEntries))
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = sqlCommand;

                    Execute(command);
                }
            }
        }

        static void Execute(IDbCommand command)
        {
            try
            {
                command.ExecuteNonQuery();
            }
            catch (OracleException exception)
            {
                throw new RebusApplicationException(exception, $@"Error executing SQL command
{command.CommandText}
");
            }
        }
    }
}
