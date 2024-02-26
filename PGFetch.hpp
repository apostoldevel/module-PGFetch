/*++

Program name:

  Apostol CRM

Module Name:

  PGFetch.hpp

Notices:

  Module: Postgres Fetch

Author:

  Copyright (c) Prepodobny Alen

  mailto: alienufo@inbox.ru
  mailto: ufocomp@gmail.com

--*/

#ifndef APOSTOL_PQ_FETCH_HPP
#define APOSTOL_PQ_FETCH_HPP
//----------------------------------------------------------------------------------------------------------------------

extern "C++" {

namespace Apostol {

    namespace Module {

        class CPGFetch;

        //--------------------------------------------------------------------------------------------------------------

        //-- CFetchHandler ---------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        class CFetchHandler: public CQueueHandler {
        private:

            CString m_RequestId;

            CJSON m_Payload;

        public:

            CFetchHandler(CQueueCollection *ACollection, const CString &RequestId, COnQueueHandlerEvent && Handler);

            ~CFetchHandler() override = default;

            const CString &RequestId() const { return m_RequestId; }

            CJSON &Payload() { return m_Payload; }
            const CJSON &Payload() const { return m_Payload; }

        };

        //--------------------------------------------------------------------------------------------------------------

        //-- CPGFetch --------------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        class CPGFetch: public CQueueCollection, public CApostolModule {
        private:

            CCURLClient m_Client;

            int m_TimeOut;

            CDateTime m_CheckDate;

            void InitListen();
            void CheckListen();

            void CheckTimeOut(CDateTime Now);

            static CJSON ParamsToJson(const CStringList &Params);
            static CJSON HeadersToJson(const CHeaders &Headers);

            void InitMethods() override;

            static void QueryException(CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E);

            static int CheckError(const CJSON &Json, CString &ErrorMessage);
            static CHTTPReply::CStatusType ErrorCodeToStatus(int ErrorCode);

            void DeleteHandler(CQueueHandler *AHandler) override;

        protected:

            void DoError(const Delphi::Exception::Exception &E);

            void DoQuery(CQueueHandler *AHandler);
            void DoFetch(CQueueHandler *AHandler);

            void DoCURL(CFetchHandler *AHandler);

            void DoDone(CFetchHandler *AHandler, const CHTTPReply &Reply);
            void DoFail(CFetchHandler *AHandler, const CString &Message);

            void DoGet(CHTTPServerConnection *AConnection) override;
            void DoPost(CHTTPServerConnection *AConnection);

            void DoCurlException(CCURLClient *Sender, const Delphi::Exception::Exception &E);

            void DoPostgresNotify(CPQConnection *AConnection, PGnotify *ANotify) override;

            void DoPostgresQueryExecuted(CPQPollQuery *APollQuery) override;
            void DoPostgresQueryException(CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) override;

            void DoConnected(CObject *Sender);
            void DoDisconnected(CObject *Sender);

        public:

            explicit CPGFetch(CModuleProcess *AProcess);

            ~CPGFetch() override = default;

            static class CPGFetch *CreateModule(CModuleProcess *AProcess) {
                return new CPGFetch(AProcess);
            }

            void Initialization(CModuleProcess *AProcess) override;

            void UnloadQueue() override;

            void PQGet(CHTTPServerConnection *AConnection, const CString &Path);
            void PQPost(CHTTPServerConnection *AConnection, const CString &Path, const CString &Body);

            void Heartbeat(CDateTime DateTime) override;

            bool Enabled() override;

            bool CheckLocation(const CLocation &Location) override;

        };

    }
}

using namespace Apostol::Module;
}
#endif //APOSTOL_PQ_FETCH_HPP
