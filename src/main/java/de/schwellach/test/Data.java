package de.schwellach.test;

import com.helixleisure.schema.DataUnit;
import com.helixleisure.schema.EquivEdge;
import com.helixleisure.schema.OrigSystem;
import com.helixleisure.schema.PageID;
import com.helixleisure.schema.PageViewEdge;
import com.helixleisure.schema.PageViewSystem;
import com.helixleisure.schema.Pedigree;
import com.helixleisure.schema.PersonID;
import com.helixleisure.schema.Source;

public class Data {
	public static Pedigree makePedigree(int timeSecs) {
        return new Pedigree(timeSecs,
                Source.SELF,
                OrigSystem.page_view(new PageViewSystem())
        );

    }

    public static com.helixleisure.schema.Data makePageview(int userid, String url, int timeSecs) {
        return new com.helixleisure.schema.Data(makePedigree(timeSecs),
                DataUnit.page_view(
                        new PageViewEdge(
                                PersonID.user_id(userid),
                                PageID.url(url),
                                1
                        )));
    }

    public static com.helixleisure.schema.Data makeEquiv(int user1, int user2) {
        return new com.helixleisure.schema.Data(makePedigree(1000),
                DataUnit.equiv(
                        new EquivEdge(
                                PersonID.user_id(user1),
                                PersonID.user_id(user2)
                        )));
    }
}
