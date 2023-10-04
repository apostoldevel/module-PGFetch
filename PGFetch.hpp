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
        class CFetchThread;
        class CFetchThreadMgr;

        //--------------------------------------------------------------------------------------------------------------

        //-- CCurlFetch ------------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        class CCurlFetch: public CCurlApi {
        private:

            mutable CStringList m_Into;

        protected:

            void CurlInfo() const override;

        public:

            CCurlFetch();
            ~CCurlFetch() override = default;

            int GetResponseCode() const;

        };

        //--------------------------------------------------------------------------------------------------------------

        //-- CFetchHandler ---------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        class CFetchHandler: public CQueueHandler {
        private:

            CFetchThread *m_pThread;

            CString m_RequestId;

            CJSON m_Payload;

        public:

            CFetchHandler(CQueueCollection *ACollection, const CString &RequestId, COnQueueHandlerEvent && Handler);

            ~CFetchHandler() override = default;

            const CString &RequestId() const { return m_RequestId; }

            CFetchThread *Thread() const { return m_pThread; };
            void SetThread(CFetchThread *AThread) { m_pThread = AThread; };

            CJSON &Payload() { return m_Payload; }
            const CJSON &Payload() const { return m_Payload; }

        };

        //--------------------------------------------------------------------------------------------------------------

        //-- CFetchThread ----------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        class CFetchThread: public CThread, public CGlobalComponent {
        private:

            CPGFetch *m_pFetch;

        protected:

            CFetchHandler *m_pHandler;
            CFetchThreadMgr *m_pThreadMgr;

        public:

            explicit CFetchThread(CPGFetch *AFetch, CFetchHandler *AHandler, CFetchThreadMgr *AThreadMgr);

            ~CFetchThread() override;

            void Execute() override;

            void TerminateAndWaitFor();

            CFetchHandler *Handler() { return m_pHandler; };
            void Handler(CFetchHandler *Value) { m_pHandler = Value; };

        };

        //--------------------------------------------------------------------------------------------------------------

        //-- CFetchThreadMgr -------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        class CFetchThreadMgr {
        protected:

            CThreadList m_ActiveThreads;
            CThreadPriority m_ThreadPriority;

        public:

            CFetchThreadMgr();

            virtual ~CFetchThreadMgr();

            virtual CFetchThread *GetThread(CPGFetch *AFetch, CFetchHandler *AHandler);

            virtual void ReleaseThread(CFetchThread *AThread) abstract;

            void TerminateThreads();

            CThreadList &ActiveThreads() { return m_ActiveThreads; }
            const CThreadList &ActiveThreads() const { return m_ActiveThreads; }

            CThreadPriority ThreadPriority() const { return m_ThreadPriority; }
            void ThreadPriority(CThreadPriority Value) { m_ThreadPriority = Value; }

        }; // CFetchThreadMgr

        //--------------------------------------------------------------------------------------------------------------

        //-- CFetchThreadMgrDefault ------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        class CFetchThreadMgrDefault : public CFetchThreadMgr {
            typedef CFetchThreadMgr inherited;

        public:

            ~CFetchThreadMgrDefault() override {
                TerminateThreads();
            };

            CFetchThread *GetThread(CPGFetch *AFetch, CFetchHandler *AHandler) override {
                return inherited::GetThread(AFetch, AHandler);
            };

            void ReleaseThread(CFetchThread *AThread) override {
                if (!IsCurrentThread(AThread)) {
                    AThread->FreeOnTerminate(false);
                    AThread->TerminateAndWaitFor();
                    FreeAndNil(AThread);
                } else {
                    AThread->FreeOnTerminate(true);
                    AThread->Terminate();
                }
            };

        };

        //--------------------------------------------------------------------------------------------------------------

        //-- CPGFetch --------------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        class CPGFetch: public CQueueCollection, public CApostolModule {
        private:

            int m_TimeOut;

            CDateTime m_CheckDate;

            CFetchThreadMgrDefault m_ThreadMgr;

            void InitListen();
            void CheckListen();

            void CheckTimeOut(CDateTime Now);

            CFetchThread *GetThread(CFetchHandler *AHandler);

            static CJSON ParamsToJson(const CStringList &Params);
            static CJSON HeadersToJson(const CHeaders &Headers);

            void InitMethods() override;

            static void QueryException(CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E);

            static int CheckError(const CJSON &Json, CString &ErrorMessage);
            static CHTTPReply::CStatusType ErrorCodeToStatus(int ErrorCode);

            void DeleteHandler(CQueueHandler *AHandler) override;

        protected:

            static void DoError(const Delphi::Exception::Exception &E);

            void DoQuery(CQueueHandler *AHandler);
            void DoFetch(CQueueHandler *AHandler);

            void DoThread(CFetchHandler *AHandler);

            void DoDone(CFetchHandler *AHandler, const CHTTPReply &Reply);
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

            void Initialization(CModuleProcess *AProcess) override;

            void UnloadQueue() override;

            void PQGet(CHTTPServerConnection *AConnection, const CString &Path);
            void PQPost(CHTTPServerConnection *AConnection, const CString &Path, const CString &Body);

            void Heartbeat(CDateTime DateTime) override;

            bool Enabled() override;

            bool CheckLocation(const CLocation &Location) override;

            void CURL(CQueueHandler *AHandler);

        };

    }
}

using namespace Apostol::Module;
}
#endif //APOSTOL_PQ_FETCH_HPP
