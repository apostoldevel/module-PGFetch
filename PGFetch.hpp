/*++

Program name:

  Apostol Web Service

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
        class CFetchHandler;
        //--------------------------------------------------------------------------------------------------------------

        typedef std::function<void (CFetchHandler *Handler)> COnFetchHandlerEvent;
        //--------------------------------------------------------------------------------------------------------------

        class CFetchHandler: public CPollConnection {
        private:

            CPGFetch *m_pModule;

            bool m_Allow;

            CJSON m_Payload;

            COnFetchHandlerEvent m_Handler;

            int AddToQueue();
            void RemoveFromQueue();

        protected:

            void SetAllow(bool Value) { m_Allow = Value; }

        public:

            CFetchHandler(CPGFetch *AModule, const CString &Data, COnFetchHandlerEvent && Handler);

            ~CFetchHandler() override;

            const CJSON &Payload() const { return m_Payload; }

            bool Allow() const { return m_Allow; };
            void Allow(bool Value) { SetAllow(Value); };

            bool Handler();

            void Close() override;

        };

        //--------------------------------------------------------------------------------------------------------------

        //-- CPGFetch --------------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        typedef CPollManager CQueueManager;
        //--------------------------------------------------------------------------------------------------------------

        class CPGFetch: public CApostolModule {
        private:

            CQueue m_Queue;
            CQueueManager m_QueueManager;

            CDateTime m_CheckDate;

            size_t m_Progress;
            size_t m_MaxQueue;

            void InitListen();
            void CheckListen();

            void UnloadQueue();
            void CheckTimeOut(CDateTime Now);

            void DeleteHandler(CFetchHandler *AHandler);

            static CJSON ParamsToJson(const CStringList &Params);
            static CJSON HeadersToJson(const CHeaders &Headers);

            void InitMethods() override;

            static void QueryException(CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E);

            static int CheckError(const CJSON &Json, CString &ErrorMessage);
            static CHTTPReply::CStatusType ErrorCodeToStatus(int ErrorCode);

        protected:

            static void DoError(const Delphi::Exception::Exception &E);

            void DoFetch(CFetchHandler *AHandler);
            void DoDone(CFetchHandler *AHandler, CHTTPReply *Reply);
            void DoFail(CFetchHandler *AHandler, const CString &Message);

            void DoGet(CHTTPServerConnection *AConnection) override;
            void DoPost(CHTTPServerConnection *AConnection);

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

            void PQGet(CHTTPServerConnection *AConnection, const CString &Path);
            void PQPost(CHTTPServerConnection *AConnection, const CString &Path, const CString &Body);

            void Heartbeat(CDateTime DateTime) override;

            bool Enabled() override;

            bool CheckLocation(const CLocation &Location) override;

            void IncProgress() { m_Progress++; }
            void DecProgress() { m_Progress--; }

            int AddToQueue(CFetchHandler *AHandler);
            void InsertToQueue(int Index, CFetchHandler *AHandler);
            void RemoveFromQueue(CFetchHandler *AHandler);

            CQueue &Queue() { return m_Queue; }
            const CQueue &Queue() const { return m_Queue; }

            CPollManager *ptrQueueManager() { return &m_QueueManager; }

            CPollManager &QueueManager() { return m_QueueManager; }
            const CPollManager &QueueManager() const { return m_QueueManager; }

        };
    }
}

using namespace Apostol::Module;
}
#endif //APOSTOL_PQ_FETCH_HPP
