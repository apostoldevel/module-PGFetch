/*++

Program name:

  Apostol CRM

Module Name:

  PGFetch.cpp

Notices:

  Module: Postgres Fetch

Author:

  Copyright (c) Prepodobny Alen

  mailto: alienufo@inbox.ru
  mailto: ufocomp@gmail.com

--*/

//----------------------------------------------------------------------------------------------------------------------

#include "Core.hpp"
#include "PGFetch.hpp"
//----------------------------------------------------------------------------------------------------------------------

#define FETCH_TIMEOUT_INTERVAL 15000
#define PG_CONFIG_NAME "helper"
#define PG_LISTEN_NAME "http"
#define PG_FETCH_HEADER_ATTACHE_FILE "x-attache-file"
//----------------------------------------------------------------------------------------------------------------------

extern "C++" {

namespace Apostol {

    namespace Module {

        CString PQQuoteLiteralJson(const CString &Json) {

            if (Json.IsEmpty())
                return "null";

            CString Result;
            TCHAR ch;

            Result = "'";

            for (size_t Index = 0; Index < Json.Size(); Index++) {
                ch = Json.at(Index);
                if (ch == '\'')
                    Result.Append('\'');
                Result.Append(ch);
            }

            Result << "'";

            return Result;
        }

        //--------------------------------------------------------------------------------------------------------------

        //-- CFetchHandler ---------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        CFetchHandler::CFetchHandler(CQueueCollection *ACollection, const CString &RequestId, COnQueueHandlerEvent && Handler):
                CQueueHandler(ACollection, static_cast<COnQueueHandlerEvent &&> (Handler)) {

            m_TimeOut = INFINITE;
            m_TimeOutInterval = FETCH_TIMEOUT_INTERVAL;

            m_RequestId = RequestId;

            m_pThread = nullptr;
        }

        //--------------------------------------------------------------------------------------------------------------

        //-- CFetchThread ----------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        CFetchThread::CFetchThread(CPGFetch *AFetch, CFetchHandler *AHandler, CFetchThreadMgr *AThreadMgr):
                CThread(true), CGlobalComponent() {

            m_pFetch = AFetch;
            m_pHandler = AHandler;
            m_pThreadMgr = AThreadMgr;

            if (Assigned(m_pHandler))
                m_pHandler->SetThread(this);

            if (Assigned(m_pThreadMgr))
                m_pThreadMgr->ActiveThreads().Add(this);
        }
        //--------------------------------------------------------------------------------------------------------------

        CFetchThread::~CFetchThread() {
            if (Assigned(m_pHandler)) {
                m_pHandler->SetThread(nullptr);
                m_pHandler = nullptr;
            }

            if (Assigned(m_pThreadMgr))
                m_pThreadMgr->ActiveThreads().Remove(this);

            m_pThreadMgr = nullptr;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFetchThread::TerminateAndWaitFor() {
            if (FreeOnTerminate())
                throw Delphi::Exception::Exception(_T("Cannot call TerminateAndWaitFor on FreeAndTerminate threads."));

            Terminate();
            if (Suspended())
                Resume();

            WaitFor();
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFetchThread::Execute() {
            m_pFetch->CURL(m_pHandler);
        }

        //--------------------------------------------------------------------------------------------------------------

        //-- CFetchThreadMgr -------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        CFetchThreadMgr::CFetchThreadMgr() {
            m_ThreadPriority = tpNormal;
        }
        //--------------------------------------------------------------------------------------------------------------

        CFetchThreadMgr::~CFetchThreadMgr() {
            TerminateThreads();
        }
        //--------------------------------------------------------------------------------------------------------------

        CFetchThread *CFetchThreadMgr::GetThread(CPGFetch *AFetch, CFetchHandler *AHandler) {
            return new CFetchThread(AFetch, AHandler, this);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFetchThreadMgr::TerminateThreads() {
            while (m_ActiveThreads.List().Count() > 0) {
                auto pThread = static_cast<CFetchThread *> (m_ActiveThreads.List().Last());
                ReleaseThread(pThread);
            }
        }

        //--------------------------------------------------------------------------------------------------------------

        //-- CPGFetch --------------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        CPGFetch::CPGFetch(CModuleProcess *AProcess): CQueueCollection(Config()->PostgresPollMin()),
                CApostolModule(AProcess, "pg fetch", "module/PGFetch") {

            m_Headers.Add("Authorization");

            m_CheckDate = 0;
            m_Progress = 0;
            m_TimeOut = 0;

            CPGFetch::InitMethods();
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::InitMethods() {
#if defined(_GLIBCXX_RELEASE) && (_GLIBCXX_RELEASE >= 9)
            m_Methods.AddObject(_T("GET")    , (CObject *) new CMethodHandler(true , [this](auto && Connection) { DoGet(Connection); }));
            m_Methods.AddObject(_T("POST")   , (CObject *) new CMethodHandler(true , [this](auto && Connection) { DoPost(Connection); }));
            m_Methods.AddObject(_T("OPTIONS"), (CObject *) new CMethodHandler(true , [this](auto && Connection) { DoOptions(Connection); }));
            m_Methods.AddObject(_T("HEAD")   , (CObject *) new CMethodHandler(false, [this](auto && Connection) { MethodNotAllowed(Connection); }));
            m_Methods.AddObject(_T("PUT")    , (CObject *) new CMethodHandler(false, [this](auto && Connection) { MethodNotAllowed(Connection); }));
            m_Methods.AddObject(_T("DELETE") , (CObject *) new CMethodHandler(false, [this](auto && Connection) { MethodNotAllowed(Connection); }));
            m_Methods.AddObject(_T("TRACE")  , (CObject *) new CMethodHandler(false, [this](auto && Connection) { MethodNotAllowed(Connection); }));
            m_Methods.AddObject(_T("PATCH")  , (CObject *) new CMethodHandler(false, [this](auto && Connection) { MethodNotAllowed(Connection); }));
            m_Methods.AddObject(_T("CONNECT"), (CObject *) new CMethodHandler(false, [this](auto && Connection) { MethodNotAllowed(Connection); }));
#else
            m_Methods.AddObject(_T("GET")    , (CObject *) new CMethodHandler(true , std::bind(&CPGFetch::DoGet, this, _1)));
            m_Methods.AddObject(_T("POST")   , (CObject *) new CMethodHandler(true , std::bind(&CPGFetch::DoPost, this, _1)));
            m_Methods.AddObject(_T("OPTIONS"), (CObject *) new CMethodHandler(true , std::bind(&CPGFetch::DoOptions, this, _1)));
            m_Methods.AddObject(_T("HEAD")   , (CObject *) new CMethodHandler(false, std::bind(&CPGFetch::MethodNotAllowed, this, _1)));
            m_Methods.AddObject(_T("PUT")    , (CObject *) new CMethodHandler(false, std::bind(&CPGFetch::MethodNotAllowed, this, _1)));
            m_Methods.AddObject(_T("DELETE") , (CObject *) new CMethodHandler(false, std::bind(&CPGFetch::MethodNotAllowed, this, _1)));
            m_Methods.AddObject(_T("TRACE")  , (CObject *) new CMethodHandler(false, std::bind(&CPGFetch::MethodNotAllowed, this, _1)));
            m_Methods.AddObject(_T("PATCH")  , (CObject *) new CMethodHandler(false, std::bind(&CPGFetch::MethodNotAllowed, this, _1)));
            m_Methods.AddObject(_T("CONNECT"), (CObject *) new CMethodHandler(false, std::bind(&CPGFetch::MethodNotAllowed, this, _1)));
#endif
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::QueryException(CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
            auto pConnection = dynamic_cast<CHTTPServerConnection *> (APollQuery->Binding());
            ReplyError(pConnection, CHTTPReply::internal_server_error, E.what());
        }
        //--------------------------------------------------------------------------------------------------------------

        int CPGFetch::CheckError(const CJSON &Json, CString &ErrorMessage) {
            int errorCode = 0;

            if (Json.HasOwnProperty(_T("error"))) {
                const auto& error = Json[_T("error")];

                if (error.HasOwnProperty(_T("code"))) {
                    errorCode = error[_T("code")].AsInteger();
                } else {
                    return 0;
                }

                if (error.HasOwnProperty(_T("message"))) {
                    ErrorMessage = error[_T("message")].AsString();
                } else {
                    return 0;
                }

                if (errorCode >= 10000)
                    errorCode = errorCode / 100;

                if (errorCode < 0)
                    errorCode = 400;
            }

            return errorCode;
        }
        //--------------------------------------------------------------------------------------------------------------

        CHTTPReply::CStatusType CPGFetch::ErrorCodeToStatus(int ErrorCode) {
            CHTTPReply::CStatusType status = CHTTPReply::ok;

            if (ErrorCode != 0) {
                switch (ErrorCode) {
                    case 401:
                        status = CHTTPReply::unauthorized;
                        break;

                    case 403:
                        status = CHTTPReply::forbidden;
                        break;

                    case 404:
                        status = CHTTPReply::not_found;
                        break;

                    case 500:
                        status = CHTTPReply::internal_server_error;
                        break;

                    default:
                        status = CHTTPReply::bad_request;
                        break;
                }
            }

            return status;
        }
        //--------------------------------------------------------------------------------------------------------------

        CJSON CPGFetch::ParamsToJson(const CStringList &Params) {
            CJSON Json;
            for (int i = 0; i < Params.Count(); i++) {
                Json.Object().AddPair(Params.Names(i), Params.Values(i));
            }
            return Json;
        }
        //--------------------------------------------------------------------------------------------------------------

        CJSON CPGFetch::HeadersToJson(const CHeaders &Headers) {
            CJSON Json;
            for (int i = 0; i < Headers.Count(); i++) {
                const auto &caHeader = Headers[i];
                Json.Object().AddPair(caHeader.Name(), caHeader.Value());
            }
            return Json;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::DoError(const Delphi::Exception::Exception &E) {
            Log()->Error(APP_LOG_ERR, 0, "[PGFetch] Error: %s", E.what());
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::DeleteHandler(CQueueHandler *AHandler) {
            auto pHandler = dynamic_cast<CFetchHandler *> (AHandler);
            if (Assigned(pHandler)) {
                pHandler->Unlock();
                CQueueCollection::DeleteHandler(AHandler);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::DoConnected(CObject *Sender) {
            auto pConnection = dynamic_cast<CHTTPClientConnection *>(Sender);
            if (Assigned(pConnection)) {
                auto pSocket = pConnection->Socket();
                if (pSocket != nullptr) {
                    auto pHandle = pSocket->Binding();
                    if (pHandle != nullptr) {
                        Log()->Notice(_T("[PGFetch] [%s:%d] Client connected."), pHandle->PeerIP(), pHandle->PeerPort());
                    }
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::DoDisconnected(CObject *Sender) {
            auto pConnection = dynamic_cast<CHTTPClientConnection *>(Sender);
            if (Assigned(pConnection)) {
                auto pSocket = pConnection->Socket();
                if (pSocket != nullptr) {
                    auto pHandle = pSocket->Binding();
                    if (pHandle != nullptr) {
                        Log()->Notice(_T("[PGFetch] [%s:%d] Client disconnected."), pHandle->PeerIP(), pHandle->PeerPort());
                    }
                } else {
                    Log()->Notice(_T("[PGFetch] Client disconnected."));
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::DoPostgresNotify(CPQConnection *AConnection, PGnotify *ANotify) {
            DebugNotify(AConnection, ANotify);

            if (CompareString(ANotify->relname, PG_LISTEN_NAME) == 0) {
#if defined(_GLIBCXX_RELEASE) && (_GLIBCXX_RELEASE >= 9)
                new CFetchHandler(this, ANotify->extra, [this](auto &&Handler) { DoQuery(Handler); });
#else
                new CFetchHandler(this, ANotify->extra, std::bind(&CPGFetch::DoQuery, this, _1));
#endif
                UnloadQueue();
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::DoPostgresQueryExecuted(CPQPollQuery *APollQuery) {

            auto pResult = APollQuery->Results(0);

            try {
                if (pResult->ExecStatus() != PGRES_TUPLES_OK)
                    throw Delphi::Exception::EDBError(pResult->GetErrorMessage());

                CString errorMessage;

                auto pConnection = dynamic_cast<CHTTPServerConnection *> (APollQuery->Binding());

                if (pConnection != nullptr && !pConnection->ClosedGracefully()) {
                    const auto &caRequest = pConnection->Request();
                    auto &Reply = pConnection->Reply();

                    CStringList ResultObject;
                    CStringList ResultFormat;

                    ResultObject.Add("true");
                    ResultObject.Add("false");

                    ResultFormat.Add("object");
                    ResultFormat.Add("array");
                    ResultFormat.Add("null");

                    const auto &result_object = caRequest.Params[_T("result_object")];
                    const auto &result_format = caRequest.Params[_T("result_format")];

                    if (!result_object.IsEmpty() && ResultObject.IndexOfName(result_object) == -1) {
                        ReplyError(pConnection, CHTTPReply::bad_request, CString().Format("Invalid result_object: %s", result_object.c_str()));
                        return;
                    }

                    if (!result_format.IsEmpty() && ResultFormat.IndexOfName(result_format) == -1) {
                        ReplyError(pConnection, CHTTPReply::bad_request, CString().Format("Invalid result_format: %s", result_format.c_str()));
                        return;
                    }

                    CHTTPReply::CStatusType status = CHTTPReply::ok;

                    try {
                        if (pResult->nTuples() == 1) {
                            const CJSON Payload(pResult->GetValue(0, 0));
                            status = ErrorCodeToStatus(CheckError(Payload, errorMessage));
                        }

                        PQResultToJson(pResult, Reply.Content, result_format, result_object == "true" ? "result" : CString());
                    } catch (Delphi::Exception::Exception &E) {
                        errorMessage = E.what();
                        status = CHTTPReply::bad_request;
                        Log()->Error(APP_LOG_ERR, 0, "%s", E.what());
                    }

                    if (status == CHTTPReply::ok) {
                        pConnection->SendReply(status, nullptr, true);
                    } else {
                        ReplyError(pConnection, status, errorMessage);
                    }
                }
            } catch (Delphi::Exception::Exception &E) {
                QueryException(APollQuery, E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::DoPostgresQueryException(CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
            QueryException(APollQuery, E);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::DoDone(CFetchHandler *AHandler, const CHTTPReply &Reply) {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                auto pHandler = dynamic_cast<CFetchHandler *> (APollQuery->Binding());
                DeleteHandler(pHandler);
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                auto pHandler = dynamic_cast<CFetchHandler *> (APollQuery->Binding());
                DeleteHandler(pHandler);
                DoError(E);
            };

            const auto &caPayload = AHandler->Payload();

            const auto &caHeaders = PQQuoteLiteral(HeadersToJson(Reply.Headers).ToString());
            const auto &caContent = PQQuoteLiteral(base64_encode(Reply.Content));

            const auto &caRequest = caPayload["id"].AsString();
            const auto &caDone = caPayload["done"];

            CStringList SQL;

            SQL.Add(CString()
                            .MaxFormatSize(256 + caRequest.Size() + caHeaders.Size() + caContent.Size())
                            .Format("SELECT http.create_response(%s::uuid, %d, %s, %s::jsonb, decode(%s, 'base64'));",
                                    PQQuoteLiteral(caRequest).c_str(),
                                    (int) Reply.Status,
                                    PQQuoteLiteral(Reply.StatusText).c_str(),
                                    caHeaders.c_str(),
                                    caContent.c_str()
                            ));

            if (!caDone.IsNull()) {
                SQL.Add(CString().Format("SELECT %s(%s::uuid);", caDone.AsString().c_str(), PQQuoteLiteral(caRequest).c_str()));
            }

            try {
                ExecSQL(SQL, AHandler, OnExecuted, OnException);
            } catch (Delphi::Exception::Exception &E) {
                DeleteHandler(AHandler);
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::DoFail(CFetchHandler *AHandler, const CString &Message) {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                auto pHandler = dynamic_cast<CFetchHandler *> (APollQuery->Binding());
                DeleteHandler(pHandler);
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                auto pHandler = dynamic_cast<CFetchHandler *> (APollQuery->Binding());
                DeleteHandler(pHandler);
                DoError(E);
            };

            const auto &caPayload = AHandler->Payload();
            const auto &caRequest = caPayload["id"].AsString();
            const auto &caFail = caPayload["fail"];

            CStringList SQL;

            SQL.Add(CString()
                            .MaxFormatSize(256 + caRequest.Size() + Message.Size())
                            .Format("SELECT http.fail(%s::uuid, %s);",
                                    PQQuoteLiteral(caRequest).c_str(),
                                    PQQuoteLiteral(Message).c_str()
                            ));

            if (!caFail.IsNull()) {
                SQL.Add(CString().Format("SELECT %s(%s::uuid);", caFail.AsString().c_str(), PQQuoteLiteral(caRequest).c_str()));
            }

            try {
                ExecSQL(SQL, AHandler, OnExecuted, OnException);
            } catch (Delphi::Exception::Exception &E) {
                DeleteHandler(AHandler);
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::DoQuery(CQueueHandler *AHandler) {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                auto pHandler = dynamic_cast<CFetchHandler *> (APollQuery->Binding());

                if (pHandler == nullptr)
                    return;

                if (APollQuery->Count() == 0) {
                    DeleteHandler(pHandler);
                    return;
                }

                CPQResult *pResult;
                try {
                    for (int i = 0; i < APollQuery->Count(); i++) {
                        pResult = APollQuery->Results(i);

                        if (pResult->ExecStatus() != PGRES_TUPLES_OK)
                            throw Delphi::Exception::EDBError(pResult->GetErrorMessage());

                        CString Json;
                        Postgres::PQResultToJson(pResult, Json);

                        pHandler->Payload() = Json;
                        const auto& type = pHandler->Payload()["type"].AsString();

                        if (type == "curl") {
                            DoThread(pHandler);
                        } else {
                            DoFetch(pHandler);
                        }
                    }
                } catch (Delphi::Exception::Exception &E) {
                    DeleteHandler(pHandler);
                    DoError(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                auto pHandler = dynamic_cast<CFetchHandler *> (APollQuery->Binding());
                if (Assigned(pHandler)) {
                    DeleteHandler(pHandler);
                }
                DoError(E);
            };

            auto pHandler = dynamic_cast<CFetchHandler *> (AHandler);

            if (pHandler == nullptr)
                return;

            AHandler->Lock();

            CStringList SQL;

            SQL.Add(CString().Format("SELECT * FROM http.request(%s::uuid);",
                                    PQQuoteLiteral(pHandler->RequestId()).c_str()
                            ));

            try {
                ExecSQL(SQL, AHandler, OnExecuted, OnException);

                AHandler->Allow(false);

                IncProgress();
            } catch (Delphi::Exception::Exception &E) {
                DeleteHandler(AHandler);
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::DoFetch(CQueueHandler *AHandler) {

            auto OnRequest = [AHandler](CHTTPClient *Sender, CHTTPRequest &Request) {
                auto pHandler = dynamic_cast<CFetchHandler *> (AHandler);

                if (Assigned(pHandler)) {
                    const auto &caPayload = pHandler->Payload();

                    const auto &caMethod = caPayload["method"].AsString();
                    const auto &caHeaders = caPayload["headers"];
                    const auto &caContentType = caHeaders["Content-Type"];
                    const auto &caContent = caPayload["content"];

                    if (caMethod == "PUT" && caHeaders.HasOwnProperty(PG_FETCH_HEADER_ATTACHE_FILE)) {
                        const auto &caAttacheFile = caHeaders[PG_FETCH_HEADER_ATTACHE_FILE].AsString();
                        if (FileExists(caAttacheFile.c_str())) {
                            Request.Content.LoadFromFile(caAttacheFile);
                        }
                    } else {
                        if (!caContent.IsNull()) {
                            Request.Content = base64_decode(caContent.AsString());
                        }
                    }

                    CLocation URI(caPayload["resource"].AsString());

                    CHTTPRequest::Prepare(Request, caMethod.c_str(), URI.href().c_str(), caContentType.IsNull() ? _T("application/json") : caContentType.AsString().c_str());

                    for (int i = 0; i < caHeaders.Count(); i++) {
                        const auto &caHeader = caHeaders.Members(i);
                        if (caHeader.String() != PG_FETCH_HEADER_ATTACHE_FILE) {
                            Request.Headers.Values(caHeader.String(), caHeader.Value().AsString());
                        }
                    }
                }

                DebugRequest(Request);
            };
            //----------------------------------------------------------------------------------------------------------

            auto OnExecute = [this, AHandler](CTCPConnection *Sender) {
                auto pConnection = dynamic_cast<CHTTPClientConnection *> (Sender);
                auto &Reply = pConnection->Reply();

                DebugReply(Reply);

                auto pHandler = dynamic_cast<CFetchHandler *> (AHandler);
                if (Assigned(pHandler)) {
                    pHandler->Unlock();
                    DoDone(pHandler, Reply);
                }

                return true;
            };
            //----------------------------------------------------------------------------------------------------------

            auto OnException = [this, AHandler](CTCPConnection *Sender, const Delphi::Exception::Exception &E) {
                auto pConnection = dynamic_cast<CHTTPClientConnection *> (Sender);
                DebugReply(pConnection->Reply());

                auto pHandler = dynamic_cast<CFetchHandler *> (AHandler);
                if (Assigned(pHandler)) {
                    pHandler->Unlock();
                    DoFail(pHandler, E.what());
                }

                DoError(E);
            };
            //----------------------------------------------------------------------------------------------------------

            auto pHandler = dynamic_cast<CFetchHandler *> (AHandler);

            if (pHandler == nullptr)
                return;

            pHandler->Lock();
            pHandler->Allow(false);
            pHandler->TimeOut(0);
            pHandler->UpdateTimeOut(Now());

            const auto &caPayload = pHandler->Payload();

            CLocation URI(caPayload["resource"].AsString());

            auto pClient = GetClient(URI.hostname, URI.port);
#if defined(_GLIBCXX_RELEASE) && (_GLIBCXX_RELEASE >= 9)
            pClient->OnConnected([this](auto &&Sender) { DoConnected(Sender); });
            pClient->OnDisconnected([this](auto &&Sender) { DoDisconnected(Sender); });
#else
            pClient->OnConnected(std::bind(&CPGFetch::DoConnected, this, _1));
            pClient->OnDisconnected(std::bind(&CPGFetch::DoDisconnected, this, _1));
#endif

            pClient->OnRequest(OnRequest);
            pClient->OnExecute(OnExecute);
            pClient->OnException(OnException);

            try {
                pClient->AutoFree(true);
                pClient->Active(true);
            } catch (std::exception &e) {
                pHandler->Unlock();
                DoFail(pHandler, e.what());
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::CURL(CFetchHandler *AHandler) {

            if (AHandler == nullptr)
                return;

            CCurlFetch curl;
            CHeaders Headers;

            AHandler->Lock();

            const auto &caPayload = AHandler->Payload();

            const auto &method = caPayload["method"].AsString();

            const auto &caHeaders = caPayload["headers"];

            for (int i = 0; i < caHeaders.Count(); i++) {
                const auto &caHeader = caHeaders.Members(i);
                Headers.AddPair(caHeader.String(), caHeader.Value().AsString());
            }

            const auto &content = base64_decode(caPayload["content"].AsString());

            CLocation URI(caPayload["resource"].AsString());

            curl.TimeOut(m_TimeOut);

            const auto code = curl.Send(URI, method, content, Headers, false);

            if (code == CURLE_OK) {
                CHTTPReply Reply;
                const auto http_code = curl.GetResponseCode();

                Reply.Headers.Clear();
                for (int i = 1; i < curl.Headers().Count(); i++) {
                    const auto &Header = curl.Headers()[i];
                    Reply.AddHeader(Header.Name(), Header.Value());
                }

                Reply.StatusString = http_code;

                Reply.StatusText = Reply.StatusString;
                Reply.StringToStatus();

                Reply.Content = curl.Result();
                Reply.ContentLength = Reply.Content.Length();

                Reply.DelHeader("Transfer-Encoding");
                Reply.DelHeader("Content-Encoding");
                Reply.DelHeader("Content-Length");

                Reply.AddHeader("Content-Length", CString::ToString(Reply.ContentLength));

                DebugReply(Reply);

                AHandler->Unlock();
                DoDone(AHandler, Reply);
            } else {
                const auto &error = CCurlFetch::GetErrorMessage(code);

                Log()->Error(APP_LOG_NOTICE, 0, "[CURL] %d (%s)", (int) code, error.c_str());

                AHandler->Unlock();
                DoFail(AHandler, error);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::DoThread(CFetchHandler *AHandler) {
            try {
                auto pThread = GetThread(AHandler);

                pThread->FreeOnTerminate(true);
                pThread->Resume();
            } catch (std::exception &e) {
                DoFail(AHandler, e.what());
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::PQGet(CHTTPServerConnection *AConnection, const CString &Path) {

            const auto &caRequest = AConnection->Request();

            CStringList SQL;

            const auto &caHeaders = HeadersToJson(caRequest.Headers).ToString();
            const auto &caParams = ParamsToJson(caRequest.Params).ToString();

            SQL.Add(CString()
                            .MaxFormatSize(256 + Path.Size() + caHeaders.Size() + caParams.Size())
                            .Format("SELECT * FROM http.get(%s, %s::jsonb, %s::jsonb);",
                                    PQQuoteLiteral(Path).c_str(),
                                    PQQuoteLiteral(caHeaders).c_str(),
                                    PQQuoteLiteral(caParams).c_str()
                            ));

            try {
                ExecSQL(SQL, AConnection);
            } catch (Delphi::Exception::Exception &E) {
                AConnection->CloseConnection(true);
                ReplyError(AConnection, CHTTPReply::bad_request, E.what());
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::PQPost(CHTTPServerConnection *AConnection, const CString &Path, const CString &Body) {

            const auto &caRequest = AConnection->Request();

            CStringList SQL;

            const auto &caHeaders = HeadersToJson(caRequest.Headers).ToString();
            const auto &caParams = ParamsToJson(caRequest.Params).ToString();
            const auto &caBody = Body.IsEmpty() ? "null" : PQQuoteLiteral(Body);

            SQL.Add(CString()
                            .MaxFormatSize(256 + Path.Size() + caHeaders.Size() + caParams.Size() + caBody.Size())
                            .Format("SELECT * FROM http.post(%s, %s::jsonb, %s::jsonb, %s::jsonb);",
                                    PQQuoteLiteral(Path).c_str(),
                                    PQQuoteLiteral(caHeaders).c_str(),
                                    PQQuoteLiteral(caParams).c_str(),
                                    caBody.c_str()
                            ));

            try {
                ExecSQL(SQL, AConnection);
            } catch (Delphi::Exception::Exception &E) {
                AConnection->CloseConnection(true);
                ReplyError(AConnection, CHTTPReply::bad_request, E.what());
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::DoGet(CHTTPServerConnection *AConnection) {

            const auto &caRequest = AConnection->Request();
            auto &Reply = AConnection->Reply();

            Reply.ContentType = CHTTPReply::json;

            const auto &path = caRequest.Location.pathname;

            if (path.IsEmpty()) {
                AConnection->SendStockReply(CHTTPReply::not_found);
                return;
            }

            PQGet(AConnection, path);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::DoPost(CHTTPServerConnection *AConnection) {

            const auto &caRequest = AConnection->Request();
            auto &Reply = AConnection->Reply();

            Reply.ContentType = CHTTPReply::json;

            const auto &path = caRequest.Location.pathname;

            if (path.IsEmpty()) {
                AConnection->SendStockReply(CHTTPReply::not_found);
                return;
            }

            const auto& caContentType = caRequest.Headers[_T("Content-Type")].Lower();
            const auto bContentJson = (caContentType.Find(_T("application/json")) != CString::npos);

            CJSON Json;
            if (!bContentJson) {
                ContentToJson(caRequest, Json);
            }

            const auto& caBody = bContentJson ? caRequest.Content : Json.ToString();

            PQPost(AConnection, path, caBody);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::InitListen() {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                try {
                    auto pResult = APollQuery->Results(0);

                    if (pResult->ExecStatus() != PGRES_COMMAND_OK) {
                        throw Delphi::Exception::EDBError(pResult->GetErrorMessage());
                    }

                    APollQuery->Connection()->Listeners().Add(PG_LISTEN_NAME);
#if defined(_GLIBCXX_RELEASE) && (_GLIBCXX_RELEASE >= 9)
                    APollQuery->Connection()->OnNotify([this](auto && APollQuery, auto && ANotify) { DoPostgresNotify(APollQuery, ANotify); });
#else
                    APollQuery->Connection()->OnNotify(std::bind(&CPGFetch::DoPostgresNotify, this, _1, _2));
#endif
                } catch (Delphi::Exception::Exception &E) {
                    DoError(E);
                }
            };

            auto OnException = [](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                DoError(E);
            };

            CStringList SQL;

            SQL.Add("LISTEN " PG_LISTEN_NAME ";");

            try {
                ExecSQL(SQL, nullptr, OnExecuted, OnException, PG_CONFIG_NAME);
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::CheckListen() {
            if (!PQClient(PG_CONFIG_NAME).CheckListen(PG_LISTEN_NAME))
                InitListen();
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::UnloadQueue() {
            const auto index = m_Queue.IndexOf(this);
            if (index != -1) {
                const auto pQueue = m_Queue[index];
                for (int i = 0; i < pQueue->Count(); ++i) {
                    auto pHandler = (CFetchHandler *) pQueue->Item(i);
                    if (pHandler != nullptr && pHandler->Allow()) {
                        pHandler->Handler();
                        if (m_Progress >= m_MaxQueue) {
                            Log()->Warning("[%s] [%d] [%d] Queued is full.", ModuleName().c_str(), m_Progress, m_MaxQueue);
                            break;
                        }
                    }
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::CheckTimeOut(CDateTime Now) {
            const auto index = m_Queue.IndexOf(this);
            if (index != -1) {
                const auto pQueue = m_Queue[index];
                for (int i = pQueue->Count() - 1; i >= 0; i--) {
                    auto pHandler = (CFetchHandler *) pQueue->Item(i);
                    if (pHandler != nullptr && !pHandler->Allow()) {
                        if ((pHandler->TimeOut() != INFINITE) && (Now >= pHandler->TimeOut())) {
                            DoFail(pHandler, "Connection timed out");
                        }
                    }
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        CFetchThread *CPGFetch::GetThread(CFetchHandler *AHandler) {
            return m_ThreadMgr.GetThread(this, AHandler);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::Heartbeat(CDateTime DateTime) {
            CApostolModule::Heartbeat(DateTime);
            if (DateTime >= m_CheckDate) {
                m_CheckDate = DateTime + (CDateTime) 1 / MinsPerDay; // 1 min
                CheckListen();
            }
            UnloadQueue();
            CheckTimeOut(DateTime);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::Initialization(CModuleProcess *AProcess) {
            CApostolModule::Initialization(AProcess);
            m_TimeOut = Config()->IniFile().ReadInteger(SectionName().c_str(), "timeout", 0);
        }
        //--------------------------------------------------------------------------------------------------------------

        bool CPGFetch::Enabled() {
            if (m_ModuleStatus == msUnknown)
                m_ModuleStatus = Config()->IniFile().ReadBool(SectionName().c_str(), "enable", true) ? msEnabled : msDisabled;
            return m_ModuleStatus == msEnabled;
        }
        //--------------------------------------------------------------------------------------------------------------

        bool CPGFetch::CheckLocation(const CLocation &Location) {
            return Location.pathname.SubString(0, 5) == _T("/api/");
        }
        //--------------------------------------------------------------------------------------------------------------
    }
}
}