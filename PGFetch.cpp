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

#define PG_CONFIG_NAME "helper"
#define PG_LISTEN_NAME "http"
#define PG_FETCH_HEADER_ATTACHE_FILE "x-attache-file"
//----------------------------------------------------------------------------------------------------------------------

extern "C++" {

namespace Apostol {

    namespace Module {

        //--------------------------------------------------------------------------------------------------------------

        //-- CPGFetch --------------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        CPGFetch::CPGFetch(CModuleProcess *AProcess): CFetchCommon(AProcess, "pg fetch", "module/PGFetch") {
            m_CheckDate = 0;

            m_Client.AllocateEventHandlers(Server());
#if defined(_GLIBCXX_RELEASE) && (_GLIBCXX_RELEASE >= 9)
            m_Client.OnException([this](auto &&Sender, auto &&E) { DoCurlException(Sender, E); });
#else
            m_Client.OnException(std::bind(&CPGFetch::DoCurlException, this, _1, _2));
#endif
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::DoCurlException(CCURLClient *Sender, const Delphi::Exception::Exception &E) const {
            DoError(E);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::DoFetch(CQueueHandler *AHandler) {

            auto OnRequest = [AHandler](CHTTPClient *Sender, CHTTPRequest &Request) {
                const auto pHandler = dynamic_cast<CFetchHandler *> (AHandler);

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

                    const CLocation URI(caPayload["resource"].AsString());

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
                const auto pConnection = dynamic_cast<CHTTPClientConnection *> (Sender);
                const auto &Reply = pConnection->Reply();

                DebugReply(Reply);

                const auto pHandler = dynamic_cast<CFetchHandler *> (AHandler);
                if (Assigned(pHandler)) {
                    DoDone(pHandler, Reply);
                }

                return true;
            };
            //----------------------------------------------------------------------------------------------------------

            auto OnException = [this, AHandler](CTCPConnection *Sender, const Delphi::Exception::Exception &E) {
                const auto pConnection = dynamic_cast<CHTTPClientConnection *> (Sender);
                DebugReply(pConnection->Reply());

                const auto pHandler = dynamic_cast<CFetchHandler *> (AHandler);
                if (Assigned(pHandler)) {
                    DoFail(pHandler, E.what());
                }

                DoError(E);
            };
            //----------------------------------------------------------------------------------------------------------

            const auto pHandler = dynamic_cast<CFetchHandler *> (AHandler);

            if (pHandler == nullptr)
                return;

            pHandler->Allow(false);

            if (m_TimeOut > 0) {
                pHandler->TimeOut(0);
                pHandler->TimeOutInterval((m_TimeOut + 10) * 1000);
                pHandler->UpdateTimeOut(Now());
            }

            const auto &caPayload = pHandler->Payload();

            const CLocation URI(caPayload["resource"].AsString());

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
                DoFail(pHandler, e.what());
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::DoCURL(CFetchHandler *AHandler) {

            auto OnDone = [this, AHandler](CCurlFetch *Sender, CURLcode code, const CString &Error) {
                CHTTPReply Reply;
                const auto http_code = Sender->GetResponseCode();

                Reply.Headers.Clear();
                for (int i = 1; i < Sender->Headers().Count(); i++) {
                    const auto &Header = Sender->Headers()[i];
                    Reply.AddHeader(Header.Name(), Header.Value());
                }

                Reply.StatusString = http_code;

                Reply.StatusText = Reply.StatusString;
                Reply.StringToStatus();

                Reply.Content = Sender->Result();
                Reply.ContentLength = Reply.Content.Length();

                Reply.DelHeader("Transfer-Encoding");
                Reply.DelHeader("Content-Encoding");
                Reply.DelHeader("Content-Length");

                Reply.AddHeader("Content-Length", CString::ToString(Reply.ContentLength));

                DoDone(AHandler, Reply);
            };
            //----------------------------------------------------------------------------------------------------------

            auto OnFail = [this, AHandler](CCurlFetch *Sender, CURLcode code, const CString &Error) {
                Log()->Warning("[%s] [CURL] %d (%s)", ModuleName().c_str(), (int) code, Error.c_str());
                DoFail(AHandler, Error);
            };
            //----------------------------------------------------------------------------------------------------------

            auto OnWrite = [this, AHandler](CCurlApi *Sender, LPCTSTR buffer, size_t size) {
                DoStream(AHandler, CString(buffer, size));
            };
            //----------------------------------------------------------------------------------------------------------

            CHeaders Headers;
            CString Content;

            AHandler->Allow(false);

            if (m_TimeOut > 0) {
                AHandler->TimeOut(0);
                AHandler->TimeOutInterval((m_TimeOut + 10) * 1000);
                AHandler->UpdateTimeOut(Now());
            }

            const auto &caPayload = AHandler->Payload();

            CLocation URI(caPayload["resource"].AsString());

            const auto &caMethod = caPayload["method"].AsString();
            const auto &caHeaders = caPayload["headers"];
            const auto &caContent = caPayload["content"];
            const auto &caStream = caPayload["stream"];

            if (caMethod == "PUT" && caHeaders.HasOwnProperty(PG_FETCH_HEADER_ATTACHE_FILE)) {
                const auto &caAttacheFile = caHeaders[PG_FETCH_HEADER_ATTACHE_FILE].AsString();
                if (FileExists(caAttacheFile.c_str())) {
                    Content.LoadFromFile(caAttacheFile);
                }
            } else {
                if (!caContent.IsNull()) {
                    Content = base64_decode(caContent.AsString());
                }
            }

            for (int i = 0; i < caHeaders.Count(); i++) {
                const auto &caHeader = caHeaders.Members(i);
                if (caHeader.String() != PG_FETCH_HEADER_ATTACHE_FILE) {
                    Headers.Values(caHeader.String(), caHeader.Value().AsString());
                }
            }

            try {
                if (caStream.IsNull()) {
                    m_Client.Perform(URI, caMethod, Content, Headers, OnDone, OnFail);
                } else {
                    m_Client.Perform(URI, caMethod, Content, Headers, OnDone, OnFail, OnWrite);
                };
            } catch (std::exception &e) {
                DoFail(AHandler, e.what());
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::DoQuery(CQueueHandler *AHandler) {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                const auto pHandler = dynamic_cast<CFetchHandler *> (APollQuery->Binding());

                if (pHandler == nullptr)
                    return;

                if (APollQuery->Count() == 0) {
                    DeleteHandler(pHandler);
                    return;
                }

                try {
                    for (int i = 0; i < APollQuery->Count(); i++) {
                        const auto pResult = APollQuery->Results(i);

                        if (pResult->ExecStatus() != PGRES_TUPLES_OK)
                            throw Delphi::Exception::EDBError(pResult->GetErrorMessage());

                        CString Json;
                        Postgres::PQResultToJson(pResult, Json);

                        pHandler->Payload() = Json;
                        const auto& type = pHandler->Payload()["type"].AsString();

                        if (type == "curl") {
                            DoCURL(pHandler);
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
                const auto pHandler = dynamic_cast<CFetchHandler *> (APollQuery->Binding());
                if (Assigned(pHandler)) {
                    DeleteHandler(pHandler);
                }
                DoError(E);
            };

            const auto pHandler = dynamic_cast<CFetchHandler *> (AHandler);

            if (pHandler == nullptr)
                return;

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

        void CPGFetch::InitListen() {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                try {
                    const auto pResult = APollQuery->Results(0);

                    if (pResult->ExecStatus() != PGRES_COMMAND_OK) {
                        throw Delphi::Exception::EDBError(pResult->GetErrorMessage());
                    }

                    APollQuery->Connection()->Listeners().Add(PG_LISTEN_NAME);
#if defined(_GLIBCXX_RELEASE) && (_GLIBCXX_RELEASE >= 9)
                    APollQuery->Connection()->OnNotify([this](auto && AConnection, auto && ANotify) { DoPostgresNotify(AConnection, ANotify); });
#else
                    APollQuery->Connection()->OnNotify(std::bind(&CPGFetch::DoPostgresNotify, this, _1, _2));
#endif
                } catch (Delphi::Exception::Exception &E) {
                    DoError(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
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

        void CPGFetch::Heartbeat(CDateTime DateTime) {
            CApostolModule::Heartbeat(DateTime);
            if (DateTime >= m_CheckDate) {
                m_CheckDate = DateTime + (CDateTime) 30 / SecsPerDay; // 30 sec
                CheckListen();
            }
            UnloadQueue();
            CheckTimeOut(DateTime);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CPGFetch::Initialization(CModuleProcess *AProcess) {
            CFetchCommon::Initialization(AProcess);
            m_TimeOut = Config()->IniFile().ReadInteger(SectionName().c_str(), "timeout", 0);
            m_Client.TimeOut(m_TimeOut);
        }
        //--------------------------------------------------------------------------------------------------------------

        bool CPGFetch::Enabled() {
            if (m_ModuleStatus == msUnknown)
                m_ModuleStatus = Config()->IniFile().ReadBool(SectionName().c_str(), "enable", true) ? msEnabled : msDisabled;
            return m_ModuleStatus == msEnabled;
        }
        //--------------------------------------------------------------------------------------------------------------

    }
}
}