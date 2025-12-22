import { DataTypes, Model } from 'sequelize';
import { sequelize } from '../core/utils/connections';

export interface AppsInterface extends Model {
    id: number;
    name: string;
    uid: number;
    os: number;
    platform: number;
    /**
     * @deprecated is_use_diff_text is no longer supported
     */
    is_use_diff_text: number;
    created_at: Date;
    updated_at: Date;
}

export const Apps = sequelize.define<AppsInterface>(
    'Apps',
    {
        id: {
            type: DataTypes.INTEGER({ length: 10 }),
            allowNull: false,
            autoIncrement: true,
            primaryKey: true,
        },
        name: DataTypes.STRING,
        uid: DataTypes.BIGINT({ length: 20 }),
        os: DataTypes.INTEGER({ length: 3 }),
        platform: DataTypes.INTEGER({ length: 3 }),
        is_use_diff_text: DataTypes.INTEGER({ length: 3 }),
        created_at: DataTypes.DATE,
        updated_at: DataTypes.DATE,
    },
    {
        tableName: 'apps',
        underscored: true,
        paranoid: true,
    },
);
