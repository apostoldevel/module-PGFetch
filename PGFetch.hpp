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

#include "FetchCommon.hpp"
//----------------------------------------------------------------------------------------------------------------------

extern "C++" {

namespace Apostol {

    namespace Module {

        //--------------------------------------------------------------------------------------------------------------

        //-- CPGFetch --------------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        class CPGFetch: public CFetchCommon {
        private:

            CCURLClient m_Client;

            CDateTime m_CheckDate;

            void InitListen();
            void CheckListen();

        protected:

            void DoFetch(CQueueHandler *AHandler);
            void DoCURL(CFetchHandler *AHandler);

            void DoQuery(CQueueHandler *AHandler);

            void DoPostgresNotify(CPQConnection *AConnection, PGnotify *ANotify) override;

            void DoCurlException(CCURLClient *Sender, const Delphi::Exception::Exception &E);

        public:

            explicit CPGFetch(CModuleProcess *AProcess);

            ~CPGFetch() override = default;

            static class CPGFetch *CreateModule(CModuleProcess *AProcess) {
                return new CPGFetch(AProcess);
            }

            void Initialization(CModuleProcess *AProcess) override;

            void Heartbeat(CDateTime DateTime) override;

            bool Enabled() override;

        };

    }
}

using namespace Apostol::Module;
}
#endif //APOSTOL_PQ_FETCH_HPP
